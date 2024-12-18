package ru.raiffeisen.trino.arrow.flight.sql.producer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import ru.raiffeisen.trino.arrow.flight.sql.metrics.FlightSqlMetrics;
import ru.raiffeisen.trino.arrow.flight.sql.server.FlightSQLTrinoProxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static java.util.stream.IntStream.range;
import static org.apache.arrow.adapter.jdbc.JdbcToArrow.sqlToArrowVectorIterator;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowUtils.jdbcToArrowSchema;
import static org.apache.arrow.flight.BackpressureStrategy.WaitResult.*;
import static org.apache.arrow.util.Preconditions.checkState;
import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;
import static ru.raiffeisen.trino.arrow.flight.sql.logging.LoggingUtils.setLogCtx;

/** Helper class intended primarily for use in {@link TrinoFlightSQLProducer}. */
class FlightSQLProducerHelper {

  private static final Calendar DEFAULT_CALENDAR = JdbcToArrowUtils.getUtcCalendar();
  private final Logger log;
  private final int batchSize;
  private final long waitTimeout;

  /**
   * Creates instance of helper class.
   *
   * @param logger Logger of the class where this helper is used.
   * @param targetBatchSize Batch size which is used to fetch results from ResultSet and stream them
   *     to client.
   */
  public FlightSQLProducerHelper(Logger logger, int targetBatchSize, long waitTimeout) {
    this.log = logger;
    this.batchSize = targetBatchSize;
    this.waitTimeout = waitTimeout;
  }

  /**
   * After authorisation, the user credentials are encoded and further used to create connection to
   * Trino. This allows to inherit user permissions from Trino: user can fetch only data which is
   * accessible for them in Trino.
   *
   * @param token Peer identity token containing user credentials.
   * @return Decoded user credentials as a pair(user, password).
   */
  public ImmutablePair<String, String> decodePeerIdentity(String token) {
    String[] parts =
        new String(Base64.getDecoder().decode(token), StandardCharsets.UTF_8).split(":");
    return new ImmutablePair<>(parts[0], parts[1]);
  }

  public String humanBytesCount(long bytes) {
    if (bytes < 0) {
      return "0B";
    }
    if (bytes < 1024) {
      return bytes + " B";
    }

    final String units = "KMGTPE";
    int idx = 0;
    double size = bytes / 1024.0;
    while (size > 1024.0 && idx < units.length() - 1) {
      size /= 1024.0;
      idx++;
    }
    return String.format("%.2f %siB", size, units.charAt(idx));
  }

  public void setLogCtxFromCallContext(String event, FlightProducer.CallContext context) {
    ImmutablePair<String, String> user = decodePeerIdentity(context.peerIdentity());
    setLogCtx(event, user.left);
  }

  /**
   * Utility method used to serialize Arrow vector schema to ByteBuffer.
   *
   * @param schema Arrow vector schema to be serialized.
   * @return ByteBuffer containing serialized vector schema.
   */
  public ByteBuffer serializeMetadata(final Schema schema) {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);
      return ByteBuffer.wrap(outputStream.toByteArray());
    } catch (final IOException e) {
      throw new RuntimeException("Failed to serialize schema", e);
    }
  }

  /**
   * Utility method used to serialize result metadata as ByteBuffer: converts metadata to Arrow
   * vector schema and then serializes it.
   *
   * @param resultMetaData Result metadata to serialize
   * @return ByteBuffer containing serialized result metadata.
   */
  public ByteBuffer serializeMetadata(ResultSetMetaData resultMetaData, BufferAllocator allocator)
      throws SQLException {
    JdbcToArrowConfig config = getJdbcToArrowConfig(allocator, batchSize);
    return serializeMetadata(jdbcToArrowSchema(resultMetaData, config));
  }

  /**
   * Utility method used to serialize parameter metadata as ByteBuffer: converts metadata to Arrow
   * vector schema and then serializes it.
   *
   * <p>Implementation is inherited from {@link JdbcToArrowUtils} but
   * uses custom function to convert JDBC type to Arrow type.
   *
   * @param parameterMetaData Parameter metadata to serialize
   * @return ByteBuffer containing serialized parameter metadata.
   */
  public ByteBuffer serializeMetadata(ParameterMetaData parameterMetaData) throws SQLException {
    Preconditions.checkNotNull(parameterMetaData);
    final List<Field> parameterFields = new ArrayList<>(parameterMetaData.getParameterCount());
    for (int parameterCounter = 1;
        parameterCounter <= parameterMetaData.getParameterCount();
        parameterCounter++) {
      final int jdbcDataType = parameterMetaData.getParameterType(parameterCounter);
      final int jdbcIsNullable = parameterMetaData.isNullable(parameterCounter);
      final boolean arrowIsNullable = jdbcIsNullable != ParameterMetaData.parameterNoNulls;
      final int precision = parameterMetaData.getPrecision(parameterCounter);
      final int scale = parameterMetaData.getScale(parameterCounter);
      final ArrowType arrowType =
          getArrowTypeFromJdbcType(
              new JdbcFieldInfo(jdbcDataType, precision, scale), DEFAULT_CALENDAR);
      final FieldType fieldType = new FieldType(arrowIsNullable, arrowType, /* dictionary= */ null);
      parameterFields.add(new Field(null, fieldType, null));
    }

    return serializeMetadata(new Schema(parameterFields));
  }

  /**
   * Recursive method used to check if exception is caused by {@link CancellationException}.
   *
   * @param e Exception to check
   * @return {@code true} if this exception is caused by {@link CancellationException}, otherwise
   *     {@code false}.
   */
  private boolean isCausedByCancellation(Throwable e) {
    if (e.getCause() == null) {
      return false;
    } else if (e.getCause() instanceof CancellationException) {
      return true;
    } else {
      return isCausedByCancellation(e.getCause());
    }
  }

  /**
   * Enhanced implementation of function that converts provided JDBC type to its respective {@link
   * ArrowType} counterpart.
   *
   * <p>The main difference from default implementation of this function provided in the {@link
   * JdbcToArrowUtils} is that current one provides support for more
   * JDBC types.
   *
   * @param fieldInfo the {@link JdbcFieldInfo} with information about the original JDBC type.
   * @param calendar the {@link Calendar} to use for datetime data types.
   * @return a new {@link ArrowType}.
   */
  private ArrowType getArrowTypeFromJdbcType(
      final JdbcFieldInfo fieldInfo, final Calendar calendar) {
    switch (fieldInfo.getJdbcType()) {
      case java.sql.Types.BOOLEAN:
      case java.sql.Types.BIT:
        return new ArrowType.Bool();
      case java.sql.Types.TINYINT:
        return new ArrowType.Int(8, true);
      case java.sql.Types.SMALLINT:
        return new ArrowType.Int(16, true);
      case java.sql.Types.INTEGER:
        return new ArrowType.Int(32, true);
      case java.sql.Types.BIGINT:
        return new ArrowType.Int(64, true);
      case java.sql.Types.NUMERIC:
      case java.sql.Types.DECIMAL:
        int precision = fieldInfo.getPrecision();
        int scale = fieldInfo.getScale();
        if (precision > 38) {
          return new ArrowType.Decimal(precision, scale, 256);
        } else {
          return new ArrowType.Decimal(precision, scale, 128);
        }
      case java.sql.Types.REAL:
      case java.sql.Types.FLOAT:
        return new ArrowType.FloatingPoint(SINGLE);
      case java.sql.Types.DOUBLE:
        return new ArrowType.FloatingPoint(DOUBLE);
      case java.sql.Types.CHAR:
      case java.sql.Types.NCHAR:
      case java.sql.Types.VARCHAR:
      case java.sql.Types.NVARCHAR:
      case java.sql.Types.LONGVARCHAR:
      case java.sql.Types.LONGNVARCHAR:
      case java.sql.Types.CLOB:
        return new ArrowType.Utf8();
      case java.sql.Types.DATE:
        return new ArrowType.Date(DateUnit.DAY);
      case java.sql.Types.TIME:
      case java.sql.Types.TIME_WITH_TIMEZONE:
        return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
      case java.sql.Types.TIMESTAMP:
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
        final String timezone;
        if (calendar != null) {
          timezone = calendar.getTimeZone().getID();
        } else {
          timezone = null;
        }
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, timezone);
      case java.sql.Types.BINARY:
      case java.sql.Types.VARBINARY:
      case java.sql.Types.LONGVARBINARY:
      case java.sql.Types.BLOB:
        return new ArrowType.Binary();
      case java.sql.Types.ARRAY:
        return new ArrowType.List();
      case java.sql.Types.NULL:
        return new ArrowType.Null();
      case java.sql.Types.STRUCT:
        return new ArrowType.Struct();
      default:
        throw new UnsupportedOperationException("Unmapped JDBC type: " + fieldInfo.getJdbcType());
    }
  }

  /**
   * Creates instance of JdbcToArrow configuration used to stream results from a ResultSet to a
   * client. The issue is that JdbcToArrowConfig does not have a public constructors. Thus, in order
   * to set a desired {@link FlightSQLProducerHelper#batchSize} we use a reflection to retrieve a
   * suitable constructor.
   *
   * @param allocator Allocator which will be used to buffer results while streaming.
   * @param targetBatchSize Desired batch size used to fetch results from ResultSet.
   * @return Instance of JdbcToArrowConfig
   */
  private JdbcToArrowConfig getJdbcToArrowConfig(BufferAllocator allocator, int targetBatchSize) {
    try {
      Function<JdbcFieldInfo, ArrowType> jdbcToArrowTypeConverter =
          (jdbcFieldInfo) -> getArrowTypeFromJdbcType(jdbcFieldInfo, DEFAULT_CALENDAR);

      Constructor<JdbcToArrowConfig> configConstructor =
          JdbcToArrowConfig.class.getDeclaredConstructor(
              BufferAllocator.class,
              Calendar.class,
              boolean.class,
              boolean.class,
              Map.class,
              Map.class,
              int.class,
              Function.class);

      configConstructor.setAccessible(true);
      return configConstructor.newInstance(
          allocator,
          DEFAULT_CALENDAR,
          false,
          false,
          null,
          null,
          targetBatchSize,
          jdbcToArrowTypeConverter);
    } catch (NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException e) {
      throw new RuntimeException("Failed to create JdbcToArrowConfig", e);
    }
  }

  /**
   * Streams data from result set to the client. This where the actual data transfer from Trino to
   * Client via this {@link FlightSQLTrinoProxy} is described.
   *
   * <p>Time taken to execute query on Trino side is excluded from consideration. Here we are only
   * interested in time that is taken to stream rows fom Trino to the client via this server.
   *
   * @param resultSet Results set which will yield data from Trino
   * @param allocator Buffer allocator used to load results.
   * @param listener Current stream listener
   * @return Two metrics: number of rows streamed and total time took to stream these rows.
   */
  public ImmutablePair<Integer, Long> streamResultSet(
      ResultSet resultSet, BufferAllocator allocator, FlightProducer.ServerStreamListener listener)
      throws SQLException, IOException {

    final BackpressureStrategyImpl bpStrategy = new BackpressureStrategyImpl();
    bpStrategy.register(listener);

    JdbcToArrowConfig config = getJdbcToArrowConfig(allocator, batchSize);
    final Schema schema = jdbcToArrowSchema(resultSet.getMetaData(), config);

    int numRows = 0;
    long streamingTime = 0;
    long startTime = 0;
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      listener.start(root);
      final VectorLoader loader = new VectorLoader(root);

      try (final ArrowVectorIterator iterator = sqlToArrowVectorIterator(resultSet, config)) {
        while (iterator.hasNext()) {
          BackpressureStrategy.WaitResult listenerStatus = bpStrategy.waitForListener(waitTimeout);

          if (listenerStatus == CANCELLED) {
            log.warn("Stream was cancelled by the client. Stop streaming data.");
            break;
          } else if (listenerStatus == TIMEOUT) {
            log.warn(
                "Timed out while waiting for listener to be ready sending data. Stop streaming data.");
            break;
          } else if (listenerStatus == OTHER) {
            log.warn("Listener wait was interrupted! stop Streaming data.");
            break;
          }

          try (final VectorSchemaRoot batchRoot = iterator.next()) {
            if (batchRoot.getRowCount() == 0) {
              break;
            }

            // start measuring time that is taken to stream data
            // only when the first non-zero batch from
            // ArrowVectorIterator is retrieved:
            if (startTime == 0) {
              log.debug("Starting to stream result set data...");
              startTime = System.currentTimeMillis();
            }

            final VectorUnloader unloader = new VectorUnloader(batchRoot);

            try (final ArrowRecordBatch batch = unloader.getRecordBatch()) {
              loader.load(batch);
              numRows += batch.getLength();
            }
            listener.putNext();
            batchRoot.clear();
          }
        }
      } catch (Exception e) {
        // Sometimes client closes stream NOT gracefully, what might cause RuntimeException.
        // This exception will be handled by the FlightServer, though.
        // Nevertheless, here we'd like to log some information about this exception.
        // To identify if exception was caused by stream being closed,
        // we will check if listener were cancelled or, as an edge case,
        // we recursively traverse causes of caught exception to see if `CancellationException` is
        // there.
        if (listener.isCancelled() || isCausedByCancellation(e)) {
          log.warn("Captured exception caused by stream cancellation: {}", e.getMessage());
        } else {
          log.warn("Captured exception NOT caused by stream cancellation: {}", e.getMessage());
          throw e;
        }
      } finally {
        // if some data has been sent to the client,
        // then measure time that was taken to stream that data:
        if (startTime != 0) {
          streamingTime += System.currentTimeMillis() - startTime;
        }
        Long sleepTime = bpStrategy.getSleepTime();
        FlightSqlMetrics.LISTENER_WAIT.increment(numRows, sleepTime);
        log.debug(String.format("Streamed %d rows for %.3fs.", numRows, (double) streamingTime / 1000));
        log.debug(
            String.format("Total listener wait time so far: %.2fs.", (double) sleepTime / 1000));
        root.clear();
      }
    }
    return new ImmutablePair<>(numRows, streamingTime);
  }

  public <T extends Message> FlightInfo getFlightInfoForSchema(
      final T request, final FlightDescriptor descriptor, final Schema schema) {
    final Ticket ticket = new Ticket(pack(request).toByteArray());

    final List<FlightEndpoint> endpoints =
        singletonList(new FlightEndpoint(ticket, Location.reuseConnection()));

    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
  }

  public <T extends Message> FlightInfo getFlightInfoForSchema(
      final T request,
      final FlightDescriptor descriptor,
      final ResultSetMetaData resultMetaData,
      BufferAllocator allocator)
      throws SQLException {
    JdbcToArrowConfig config = getJdbcToArrowConfig(allocator, batchSize);
    return getFlightInfoForSchema(request, descriptor, jdbcToArrowSchema(resultMetaData, config));
  }

  public VectorSchemaRoot getCatalogsRoot(final ResultSet data, final BufferAllocator allocator)
      throws SQLException {
    return getRoot(data, allocator, "catalog_name", "TABLE_CAT");
  }

  public VectorSchemaRoot getSchemasRoot(final ResultSet data, final BufferAllocator allocator)
      throws SQLException {
    final VarCharVector catalogs = new VarCharVector("catalog_name", allocator);
    final VarCharVector schemas =
        new VarCharVector(
            "db_schema_name", FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
    final List<FieldVector> vectors = ImmutableList.of(catalogs, schemas);
    vectors.forEach(FieldVector::allocateNew);
    final Map<FieldVector, String> vectorToColumnName =
        ImmutableMap.of(
            catalogs, "TABLE_CATALOG",
            schemas, "TABLE_SCHEM");
    saveToVectors(vectorToColumnName, data);
    final int rows =
        vectors.stream()
            .map(FieldVector::getValueCount)
            .findAny()
            .orElseThrow(IllegalStateException::new);
    vectors.forEach(vector -> vector.setValueCount(rows));
    return new VectorSchemaRoot(vectors);
  }

  public VectorSchemaRoot getTableTypesRoot(final ResultSet data, final BufferAllocator allocator)
      throws SQLException {
    return getRoot(data, allocator, "table_type", "TABLE_TYPE");
  }

  public VectorSchemaRoot getTablesRoot(
      final DatabaseMetaData databaseMetaData,
      final BufferAllocator allocator,
      final boolean includeSchema,
      final String catalog,
      final String schemaFilterPattern,
      final String tableFilterPattern,
      final String... tableTypes)
      throws SQLException, IOException {

    Objects.requireNonNull(allocator, "BufferAllocator cannot be null.");
    final VarCharVector catalogNameVector = new VarCharVector("catalog_name", allocator);
    final VarCharVector schemaNameVector = new VarCharVector("db_schema_name", allocator);
    final VarCharVector tableNameVector =
        new VarCharVector(
            "table_name", FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
    final VarCharVector tableTypeVector =
        new VarCharVector(
            "table_type", FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);

    final List<FieldVector> vectors = new ArrayList<>(4);
    vectors.add(catalogNameVector);
    vectors.add(schemaNameVector);
    vectors.add(tableNameVector);
    vectors.add(tableTypeVector);

    vectors.forEach(FieldVector::allocateNew);

    final Map<FieldVector, String> vectorToColumnName =
        ImmutableMap.of(
            catalogNameVector, "TABLE_CAT",
            schemaNameVector, "TABLE_SCHEM",
            tableNameVector, "TABLE_NAME",
            tableTypeVector, "TABLE_TYPE");

    try (final ResultSet data =
        Objects.requireNonNull(
                databaseMetaData,
                format("%s cannot be null.", databaseMetaData.getClass().getName()))
            .getTables(catalog, schemaFilterPattern, tableFilterPattern, tableTypes)) {

      saveToVectors(vectorToColumnName, data);
      final int rows =
          vectors.stream()
              .map(FieldVector::getValueCount)
              .findAny()
              .orElseThrow(IllegalStateException::new);
      vectors.forEach(vector -> vector.setValueCount(rows));

      if (includeSchema) {
        final VarBinaryVector tableSchemaVector =
            new VarBinaryVector(
                "table_schema",
                FieldType.notNullable(Types.MinorType.VARBINARY.getType()),
                allocator);
        tableSchemaVector.allocateNew(rows);

        try (final ResultSet columnsData =
            databaseMetaData.getColumns(catalog, schemaFilterPattern, tableFilterPattern, null)) {
          final Map<String, List<Field>> tableToFields = new HashMap<>();

          while (columnsData.next()) {
            final String catalogName = columnsData.getString("TABLE_CAT");
            final String schemaName = columnsData.getString("TABLE_SCHEM");
            final String tableName = columnsData.getString("TABLE_NAME");
            final String typeName = columnsData.getString("TYPE_NAME");
            final String fieldName = columnsData.getString("COLUMN_NAME");
            final int dataType = columnsData.getInt("DATA_TYPE");
            final boolean isNullable =
                columnsData.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;
            final int precision = columnsData.getInt("COLUMN_SIZE");
            final int scale = columnsData.getInt("DECIMAL_DIGITS");
            boolean isAutoIncrement =
                Objects.equals(columnsData.getString("IS_AUTOINCREMENT"), "YES");

            final List<Field> fields =
                tableToFields.computeIfAbsent(tableName, tableName_ -> new ArrayList<>());

            final FlightSqlColumnMetadata columnMetadata =
                new FlightSqlColumnMetadata.Builder()
                    .catalogName(catalogName)
                    .schemaName(schemaName)
                    .tableName(tableName)
                    .typeName(typeName)
                    .precision(precision)
                    .scale(scale)
                    .isAutoIncrement(isAutoIncrement)
                    .build();

            final Field field =
                new Field(
                    fieldName,
                    new FieldType(
                        isNullable,
                        getArrowTypeFromJdbcType(dataType, precision, scale),
                        null,
                        columnMetadata.getMetadataMap()),
                    null);
            fields.add(field);
          }

          for (int index = 0; index < rows; index++) {
            final String tableName = tableNameVector.getObject(index).toString();
            final Schema schema = new Schema(tableToFields.get(tableName));
            saveToVector(
                copyFrom(serializeMetadata(schema)).toByteArray(), tableSchemaVector, index);
          }
        }

        tableSchemaVector.setValueCount(rows);
        vectors.add(tableSchemaVector);
      }
    }

    return new VectorSchemaRoot(vectors);
  }

  public VectorSchemaRoot getPrimaryKeysRoot(final ResultSet data, final BufferAllocator allocator)
      throws SQLException {

    final VarCharVector catalogNameVector = new VarCharVector("catalog_name", allocator);
    final VarCharVector schemaNameVector = new VarCharVector("db_schema_name", allocator);
    final VarCharVector tableNameVector = new VarCharVector("table_name", allocator);
    final VarCharVector columnNameVector = new VarCharVector("column_name", allocator);
    final IntVector keySequenceVector = new IntVector("key_sequence", allocator);
    final VarCharVector keyNameVector = new VarCharVector("key_name", allocator);

    final List<FieldVector> vectors =
        new ArrayList<>(
            ImmutableList.of(
                catalogNameVector,
                schemaNameVector,
                tableNameVector,
                columnNameVector,
                keySequenceVector,
                keyNameVector));
    vectors.forEach(FieldVector::allocateNew);

    int rows = 0;
    for (; data.next(); rows++) {
      saveToVector(data.getString("TABLE_CAT"), catalogNameVector, rows);
      saveToVector(data.getString("TABLE_SCHEM"), schemaNameVector, rows);
      saveToVector(data.getString("TABLE_NAME"), tableNameVector, rows);
      saveToVector(data.getString("COLUMN_NAME"), columnNameVector, rows);
      final int key_seq = data.getInt("KEY_SEQ");
      saveToVector(data.wasNull() ? null : key_seq, keySequenceVector, rows);
      saveToVector(data.getString("PK_NAME"), keyNameVector, rows);
    }

    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
    vectorSchemaRoot.setRowCount(rows);

    return vectorSchemaRoot;
  }

  public VectorSchemaRoot getKeysRoot(final ResultSet data, final BufferAllocator allocator)
      throws SQLException {
    final VarCharVector pkCatalogNameVector = new VarCharVector("pk_catalog_name", allocator);
    final VarCharVector pkSchemaNameVector = new VarCharVector("pk_db_schema_name", allocator);
    final VarCharVector pkTableNameVector = new VarCharVector("pk_table_name", allocator);
    final VarCharVector pkColumnNameVector = new VarCharVector("pk_column_name", allocator);
    final VarCharVector fkCatalogNameVector = new VarCharVector("fk_catalog_name", allocator);
    final VarCharVector fkSchemaNameVector = new VarCharVector("fk_db_schema_name", allocator);
    final VarCharVector fkTableNameVector = new VarCharVector("fk_table_name", allocator);
    final VarCharVector fkColumnNameVector = new VarCharVector("fk_column_name", allocator);
    final IntVector keySequenceVector = new IntVector("key_sequence", allocator);
    final VarCharVector fkKeyNameVector = new VarCharVector("fk_key_name", allocator);
    final VarCharVector pkKeyNameVector = new VarCharVector("pk_key_name", allocator);
    final UInt1Vector updateRuleVector = new UInt1Vector("update_rule", allocator);
    final UInt1Vector deleteRuleVector = new UInt1Vector("delete_rule", allocator);

    Map<FieldVector, String> vectorToColumnName = new HashMap<>();
    vectorToColumnName.put(pkCatalogNameVector, "PKTABLE_CAT");
    vectorToColumnName.put(pkSchemaNameVector, "PKTABLE_SCHEM");
    vectorToColumnName.put(pkTableNameVector, "PKTABLE_NAME");
    vectorToColumnName.put(pkColumnNameVector, "PKCOLUMN_NAME");
    vectorToColumnName.put(fkCatalogNameVector, "FKTABLE_CAT");
    vectorToColumnName.put(fkSchemaNameVector, "FKTABLE_SCHEM");
    vectorToColumnName.put(fkTableNameVector, "FKTABLE_NAME");
    vectorToColumnName.put(fkColumnNameVector, "FKCOLUMN_NAME");
    vectorToColumnName.put(keySequenceVector, "KEY_SEQ");
    vectorToColumnName.put(updateRuleVector, "UPDATE_RULE");
    vectorToColumnName.put(deleteRuleVector, "DELETE_RULE");
    vectorToColumnName.put(fkKeyNameVector, "FK_NAME");
    vectorToColumnName.put(pkKeyNameVector, "PK_NAME");

    final VectorSchemaRoot vectorSchemaRoot =
        VectorSchemaRoot.of(
            pkCatalogNameVector,
            pkSchemaNameVector,
            pkTableNameVector,
            pkColumnNameVector,
            fkCatalogNameVector,
            fkSchemaNameVector,
            fkTableNameVector,
            fkColumnNameVector,
            keySequenceVector,
            fkKeyNameVector,
            pkKeyNameVector,
            updateRuleVector,
            deleteRuleVector);

    vectorSchemaRoot.allocateNew();
    final int rowCount = saveToVectors(vectorToColumnName, data);

    vectorSchemaRoot.setRowCount(rowCount);

    return vectorSchemaRoot;
  }

  public VectorSchemaRoot getTypeInfoRoot(
      FlightSql.CommandGetXdbcTypeInfo request, ResultSet typeInfo, final BufferAllocator allocator)
      throws SQLException {
    Preconditions.checkNotNull(allocator, "BufferAllocator cannot be null.");

    VectorSchemaRoot root =
        VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TYPE_INFO_SCHEMA, allocator);

    Map<FieldVector, String> mapper = new HashMap<>();
    mapper.put(root.getVector("type_name"), "TYPE_NAME");
    mapper.put(root.getVector("data_type"), "DATA_TYPE");
    mapper.put(root.getVector("column_size"), "PRECISION");
    mapper.put(root.getVector("literal_prefix"), "LITERAL_PREFIX");
    mapper.put(root.getVector("literal_suffix"), "LITERAL_SUFFIX");
    mapper.put(root.getVector("create_params"), "CREATE_PARAMS");
    mapper.put(root.getVector("nullable"), "NULLABLE");
    mapper.put(root.getVector("case_sensitive"), "CASE_SENSITIVE");
    mapper.put(root.getVector("searchable"), "SEARCHABLE");
    mapper.put(root.getVector("unsigned_attribute"), "UNSIGNED_ATTRIBUTE");
    mapper.put(root.getVector("fixed_prec_scale"), "FIXED_PREC_SCALE");
    mapper.put(root.getVector("auto_increment"), "AUTO_INCREMENT");
    mapper.put(root.getVector("local_type_name"), "LOCAL_TYPE_NAME");
    mapper.put(root.getVector("minimum_scale"), "MINIMUM_SCALE");
    mapper.put(root.getVector("maximum_scale"), "MAXIMUM_SCALE");
    mapper.put(root.getVector("sql_data_type"), "SQL_DATA_TYPE");
    mapper.put(root.getVector("datetime_subcode"), "SQL_DATETIME_SUB");
    mapper.put(root.getVector("num_prec_radix"), "NUM_PREC_RADIX");

    Predicate<ResultSet> predicate;
    if (request.hasDataType()) {
      predicate =
          (resultSet) -> {
            try {
              return resultSet.getInt("DATA_TYPE") == request.getDataType();
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          };
    } else {
      predicate = resultSet -> true;
    }

    int rows = saveToVectors(mapper, typeInfo, predicate);

    root.setRowCount(rows);
    return root;
  }

  private VectorSchemaRoot getRoot(
      final ResultSet data,
      final BufferAllocator allocator,
      final String fieldVectorName,
      final String columnName)
      throws SQLException {
    final VarCharVector dataVector =
        new VarCharVector(
            fieldVectorName, FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
    saveToVectors(ImmutableMap.of(dataVector, columnName), data);
    final int rows = dataVector.getValueCount();
    dataVector.setValueCount(rows);
    return new VectorSchemaRoot(singletonList(dataVector));
  }

  private void saveToVector(final Byte data, final UInt1Vector vector, final int index) {
    vectorConsumer(
        data,
        vector,
        fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, theData));
  }

  private void saveToVector(final Byte data, final BitVector vector, final int index) {
    vectorConsumer(
        data,
        vector,
        fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, theData));
  }

  private void saveToVector(final String data, final VarCharVector vector, final int index) {
    preconditionCheckSaveToVector(vector, index);
    vectorConsumer(
        data,
        vector,
        fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, new Text(theData)));
  }

  private void saveToVector(final Integer data, final IntVector vector, final int index) {
    preconditionCheckSaveToVector(vector, index);
    vectorConsumer(
        data,
        vector,
        fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, theData));
  }

  private void saveToVector(final byte[] data, final VarBinaryVector vector, final int index) {
    preconditionCheckSaveToVector(vector, index);
    vectorConsumer(
        data,
        vector,
        fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, theData));
  }

  private void preconditionCheckSaveToVector(final FieldVector vector, final int index) {
    Objects.requireNonNull(vector, "vector cannot be null.");
    checkState(index >= 0, "Index must be a positive number!");
  }

  private <T, V extends FieldVector> void vectorConsumer(
      final T data,
      final V vector,
      final Consumer<V> consumerIfNullable,
      final BiConsumer<T, V> defaultConsumer) {
    if (isNull(data)) {
      consumerIfNullable.accept(vector);
      return;
    }
    defaultConsumer.accept(data, vector);
  }

  private <T extends FieldVector> int saveToVectors(
      final Map<T, String> vectorToColumnName, final ResultSet data) throws SQLException {
    Predicate<ResultSet> alwaysTrue = (resultSet) -> true;
    return saveToVectors(vectorToColumnName, data, alwaysTrue);
  }

  @SuppressWarnings("StringSplitter")
  private <T extends FieldVector> int saveToVectors(
      final Map<T, String> vectorToColumnName,
      final ResultSet data,
      Predicate<ResultSet> resultSetPredicate)
      throws SQLException {
    Objects.requireNonNull(vectorToColumnName, "vectorToColumnName cannot be null.");
    Objects.requireNonNull(data, "data cannot be null.");
    final Set<Map.Entry<T, String>> entrySet = vectorToColumnName.entrySet();
    int rows = 0;

    while (data.next()) {
      if (!resultSetPredicate.test(data)) {
        continue;
      }
      for (final Map.Entry<T, String> vectorToColumn : entrySet) {
        final T vector = vectorToColumn.getKey();
        final String columnName = vectorToColumn.getValue();
        if (vector instanceof VarCharVector) {
          String thisData = data.getString(columnName);
          saveToVector(thisData, (VarCharVector) vector, rows);
        } else if (vector instanceof IntVector) {
          final int intValue = data.getInt(columnName);
          saveToVector(data.wasNull() ? null : intValue, (IntVector) vector, rows);
        } else if (vector instanceof UInt1Vector) {
          final byte byteValue = data.getByte(columnName);
          saveToVector(data.wasNull() ? null : byteValue, (UInt1Vector) vector, rows);
        } else if (vector instanceof BitVector) {
          final byte byteValue = data.getByte(columnName);
          saveToVector(data.wasNull() ? null : byteValue, (BitVector) vector, rows);
        } else if (vector instanceof ListVector) {
          String createParamsValues = data.getString(columnName);

          UnionListWriter writer = ((ListVector) vector).getWriter();

          BufferAllocator allocator = vector.getAllocator();
          final ArrowBuf buf = allocator.buffer(1024);

          writer.setPosition(rows);
          writer.startList();

          if (createParamsValues != null) {
            String[] split = createParamsValues.split(",");

            range(0, split.length)
                .forEach(
                    i -> {
                      byte[] bytes = split[i].getBytes(StandardCharsets.UTF_8);
                      Preconditions.checkState(
                          bytes.length < 1024,
                          "The amount of bytes is greater than what the ArrowBuf supports");
                      buf.setBytes(0, bytes);
                      writer.varChar().writeVarChar(0, bytes.length, buf);
                    });
          }
          buf.close();
          writer.endList();
        } else {
          throw CallStatus.INVALID_ARGUMENT
              .withDescription("Provided vector not supported")
              .toRuntimeException();
        }
      }
      rows++;
    }
    for (final Map.Entry<T, String> vectorToColumn : entrySet) {
      vectorToColumn.getKey().setValueCount(rows);
    }

    return rows;
  }

  private ArrowType getArrowTypeFromJdbcType(
      final int jdbcDataType, final int precision, final int scale) {
    try {
      return JdbcToArrowUtils.getArrowTypeFromJdbcType(
          new JdbcFieldInfo(jdbcDataType, precision, scale), DEFAULT_CALENDAR);
    } catch (UnsupportedOperationException ignored) {
      return ArrowType.Utf8.INSTANCE;
    }
  }
}
