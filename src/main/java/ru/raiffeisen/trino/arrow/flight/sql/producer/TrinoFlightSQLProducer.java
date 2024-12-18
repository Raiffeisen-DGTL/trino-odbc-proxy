package ru.raiffeisen.trino.arrow.flight.sql.producer;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;
import org.apache.arrow.adapter.jdbc.JdbcParameterBinder;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql.*;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import ru.raiffeisen.trino.arrow.flight.sql.config.FlightConfiguration;
import ru.raiffeisen.trino.arrow.flight.sql.connection.TrinoConnectionFactory;
import ru.raiffeisen.trino.arrow.flight.sql.metrics.FlightSqlMetrics;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.*;

import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.UUID.randomUUID;
import static org.slf4j.LoggerFactory.getLogger;
import static ru.raiffeisen.trino.arrow.flight.sql.logging.LoggingUtils.*;

public class TrinoFlightSQLProducer implements FlightSqlProducer {
  private static final Logger log = getLogger(TrinoFlightSQLProducer.class);

  private final SqlInfoBuilder sqlInfoBuilder;
  private final TrinoConnectionFactory trinoSource;

  private final FlightSQLProducerCache cache;
  private final FlightSQLProducerHelper helper;
  protected final BufferAllocator rootAllocator;

  private final ExecutorService executorService = Executors.newFixedThreadPool(10);

  public TrinoFlightSQLProducer(
      TrinoConnectionFactory trinoConnFactory,
      FlightConfiguration flightConfig,
      BufferAllocator rootAllocator) {
    this.trinoSource = trinoConnFactory;
    this.rootAllocator = rootAllocator;
    this.sqlInfoBuilder = new SqlInfoBuilder();
    this.helper =
        new FlightSQLProducerHelper(
            log, flightConfig.FLIGHT_BATCH_SIZE, flightConfig.FLIGHT_BACKPRESSURE_TIMEOUT);
    this.cache = new FlightSQLProducerCache();

    // schedule a buffer allocation metric update every 5 seconds:
    ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(
        () ->
            FlightSqlMetrics.BUFFER_MEM_ALLOCATION.update(this.rootAllocator.getAllocatedMemory()),
        1000,
        5000,
        TimeUnit.MILLISECONDS);

    sqlInfoBuilder
        .withFlightSqlServerName(trinoConnFactory.getServerName())
        .withFlightSqlServerVersion(trinoConnFactory.getServerVersion())
        .withFlightSqlServerArrowVersion(trinoConnFactory.getDriverVersion())
        .withFlightSqlServerReadOnly(flightConfig.FLIGHT_SERVER_READONLY)
        .withFlightSqlServerSql(true)
        .withFlightSqlServerSubstrait(false)
        .withFlightSqlServerTransaction(SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
        .withSqlIdentifierQuoteChar(flightConfig.FLIGHT_SQL_IDENTIFIER_QUOTE_CHAR)
        .withSqlDdlCatalog(flightConfig.FLIGHT_SQL_DDL_CATALOGS_SUPPORT)
        .withSqlDdlSchema(flightConfig.FLIGHT_SQL_DDL_SCHEMAS_SUPPORT)
        .withSqlDdlTable(flightConfig.FLIGHT_SQL_DDL_ALL_TABLES_SELECTABLE)
        .withSqlIdentifierCase(
            SqlSupportedCaseSensitivity.forNumber(
                flightConfig.FLIGHT_SQL_IDENTIFIER_CASE_SENSITIVITY))
        .withSqlQuotedIdentifierCase(
            SqlSupportedCaseSensitivity.forNumber(
                flightConfig.FLIGHT_SQL_IDENTIFIER_QUOTED_CASE_SENSITIVITY))
        .withSqlAllTablesAreSelectable(true)
        .withSqlNullOrdering(SqlNullOrdering.SQL_NULLS_SORTED_AT_END)
        .withSqlMaxColumnsInTable(flightConfig.FLIGHT_SQL_MAX_TABLE_COLUMNS);
  }

  @Override
  public void createPreparedStatement(
      ActionCreatePreparedStatementRequest request,
      CallContext context,
      StreamListener<Result> listener) {
    // Running on another thread
    Future<?> unused =
        executorService.submit(
            () -> {
              try (BufferAllocator childAllocator =
                  rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE)) {
                ImmutablePair<String, String> user =
                    helper.decodePeerIdentity(context.peerIdentity());

                setLogCtx("create_prepared_statement", user.left);
                log.info("Create prepared statement for query: {}", prepMsg(request.getQuery()));

                final String query = request.getQuery();

                // Ownership of the connection will be passed to the context. Do NOT close!
                final Connection connection = trinoSource.getConnection(user.left, user.right);
                final PreparedStatement statement =
                    connection.prepareStatement(
                        query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

                final ByteString handle = cache.put(statement, query);

                final ResultSetMetaData metaData = statement.getMetaData();
                final ByteString bytes =
                    isNull(metaData)
                        ? ByteString.EMPTY
                        : ByteString.copyFrom(helper.serializeMetadata(metaData, childAllocator));
                final ActionCreatePreparedStatementResult result =
                    ActionCreatePreparedStatementResult.newBuilder()
                        .setDatasetSchema(bytes)
                        .setParameterSchema(
                            copyFrom(helper.serializeMetadata(statement.getParameterMetaData())))
                        .setPreparedStatementHandle(handle)
                        .build();
                listener.onNext(new Result(pack(result).toByteArray()));
                log.info("Prepared statement created with hande = {}", handle.toStringUtf8());
              } catch (final SQLException e) {
                listener.onError(
                    CallStatus.INTERNAL
                        .withDescription("Failed to create prepared statement: " + e)
                        .toRuntimeException());
                return;
              } catch (final Throwable t) {
                listener.onError(
                    CallStatus.INTERNAL
                        .withDescription("Unknown error: " + t)
                        .toRuntimeException());
                return;
              } finally {
                clearLogCtx();
              }
              listener.onCompleted();
            });
  }

  @Override
  public void closePreparedStatement(
      ActionClosePreparedStatementRequest request,
      CallContext context,
      StreamListener<Result> listener) {
    // Running on another thread
    Future<?> unused =
        executorService.submit(
            () -> {
              try {
                ByteString handle = request.getPreparedStatementHandle();
                helper.setLogCtxFromCallContext("close_prepared_statement", context);
                log.info("Closing prepared statement with handle = {}", handle.toStringUtf8());
                cache.invalidate(handle);
              } catch (final Exception e) {
                listener.onError(e);
                return;
              } finally {
                clearLogCtx();
              }
              listener.onCompleted();
            });
  }

  @Override
  public FlightInfo getFlightInfoStatement(
      CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_flight_info_statement", user.left);

    try (BufferAllocator childAllocator =
        rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE)) {
      final String query = command.getQuery();
      log.info("Getting flight info statement for query: {}", prepMsg(query));

      // Ownership of the connection will be passed to the context. Do NOT close!
      final Connection connection = trinoSource.getConnection(user.left, user.right);
      final PreparedStatement statement =
          connection.prepareStatement(
              query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

      final ByteString handle = cache.put(statement, query);
      log.info("Statement handle = {}", handle.toStringUtf8());

      TicketStatementQuery ticket =
          TicketStatementQuery.newBuilder().setStatementHandle(handle).build();
      return helper.getFlightInfoForSchema(
          ticket, descriptor, statement.getMetaData(), childAllocator);
    } catch (final SQLException e) {
      log.error(
          format("There was a problem executing the prepared statement: <%s>.", e.getMessage()), e);
      throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
    } finally {
      clearLogCtx();
    }
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(
      CommandPreparedStatementQuery command, CallContext context, FlightDescriptor descriptor) {
    helper.setLogCtxFromCallContext("get_flight_info_prepared_statement", context);
    try (BufferAllocator childAllocator =
        rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE)) {
      ByteString handle = command.getPreparedStatementHandle();
      log.info(
          "Getting flight info for prepared statement with handle = {}", handle.toStringUtf8());
      PreparedStatement statement = cache.getStatement(command.getPreparedStatementHandle());
      assert statement != null;
      return helper.getFlightInfoForSchema(
          command, descriptor, statement.getMetaData(), childAllocator);
    } catch (final SQLException e) {
      log.error(
          format("There was a problem executing the prepared statement: <%s>.", e.getMessage()), e);
      throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
    } finally {
      clearLogCtx();
    }
  }

  @Override
  public SchemaResult getSchemaStatement(
      CommandStatementQuery commandStatementQuery,
      CallContext callContext,
      FlightDescriptor flightDescriptor) {
    helper.setLogCtxFromCallContext("get_schema_statement", callContext);
    log.error("UNIMPLEMENTED");
    return null;
  }

  @Override
  public void getStreamStatement(
      TicketStatementQuery ticketStatementQuery,
      CallContext context,
      ServerStreamListener listener) {
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_stream_statement", user.left);
    final ByteString handle = ticketStatementQuery.getStatementHandle();
    final PreparedStatement statement = Objects.requireNonNull(cache.getStatement(handle));
    log.info("Getting data stream for statement with handle: {}", handle.toStringUtf8());

    boolean isErrored = false;
    int rowsStreamed = 0;
    long streamingTime = 0;
    try (final ResultSet resultSet = statement.executeQuery()) {
      try (BufferAllocator childAllocator =
          rootAllocator.newChildAllocator(handle.toStringUtf8(), 0, Long.MAX_VALUE)) {
        ImmutablePair<Integer, Long> streamingResult = 
            helper.streamResultSet(resultSet, childAllocator, listener);
        rowsStreamed += streamingResult.left;
        streamingTime += streamingResult.right;
      }
    } catch (SQLException | IOException e) {
      log.error(format("Failed to stream statement: <%s>.", e.getMessage()), e);
      isErrored = true;
      listener.error(
          CallStatus.INTERNAL
              .withDescription("Failed to stream statement: " + e)
              .toRuntimeException());
    } finally {
      // listener.error(...) will also terminate stream,
      // thus we do not need to call completed() in this case.
      if (!isErrored) {
        log.info("Stream completed. Inform listener.");
        listener.completed();

        // Increment metrics only if query was successfully streamed to the client.
        FlightSqlMetrics.QUERY_COUNT.increment(user.left, rowsStreamed);
        FlightSqlMetrics.ROW_COUNT.increment(user.left, rowsStreamed, streamingTime);
      }
      cache.invalidate(handle);
      clearLogCtx();
    }
  }

  @Override
  public void getStreamPreparedStatement(
      CommandPreparedStatementQuery command, CallContext context, ServerStreamListener listener) {
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_stream_prepared_statement", user.left);
    final ByteString handle = command.getPreparedStatementHandle();
    final PreparedStatement statement = Objects.requireNonNull(cache.getStatement(handle));
    log.info("Getting data stream for prepared statement with handle: {}", handle.toStringUtf8());

    boolean isErrored = false;
    int rowsStreamed = 0;
    long streamingTime = 0;
    try (final ResultSet resultSet = statement.executeQuery()) {
      try (BufferAllocator childAllocator =
          rootAllocator.newChildAllocator(handle.toStringUtf8(), 0, Long.MAX_VALUE)) {
        ImmutablePair<Integer, Long> streamingResult = 
            helper.streamResultSet(resultSet, childAllocator, listener);
        rowsStreamed += streamingResult.left;
        streamingTime += streamingResult.right;
      }
    } catch (final SQLException | IOException e) {
      log.error(format("Failed to stream prepared statement: <%s>.", e.getMessage()), e);
      isErrored = true;
      listener.error(
          CallStatus.INTERNAL
              .withDescription("Failed to stream prepared statement: " + e)
              .toRuntimeException());
    } finally {
      // listener.error(...) will also terminate stream,
      // thus we do not need to call completed() in this case.
      if (!isErrored) {
        log.info("Stream completed. Inform listener.");
        listener.completed();

        // Increment metrics only if query was successfully streamed to the client.
        FlightSqlMetrics.QUERY_COUNT.increment(user.left, rowsStreamed);
        FlightSqlMetrics.ROW_COUNT.increment(user.left, rowsStreamed, streamingTime);
      }
      clearLogCtx();
    }
  }

  @Override
  public Runnable acceptPutStatement(
      CommandStatementUpdate command,
      CallContext context,
      FlightStream flightStream,
      StreamListener<PutResult> listener) {
    return () -> {
      final String query = command.getQuery();
      ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
      setLogCtx("accept_put_statement", user.left);
      log.info("Accepting statement with update query: {}", prepMsg(query));

      try (final BufferAllocator childAllocator =
              rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE);
          final Connection connection = trinoSource.getConnection(user.left, user.right);
          final Statement statement = connection.createStatement()) {
        final int result = statement.executeUpdate(query);

        final DoPutUpdateResult build =
            DoPutUpdateResult.newBuilder().setRecordCount(result).build();

        try (final ArrowBuf buffer = childAllocator.buffer(build.getSerializedSize())) {
          buffer.writeBytes(build.toByteArray());
          listener.onNext(PutResult.metadata(buffer));
          listener.onCompleted();
        }
      } catch (SQLSyntaxErrorException e) {
        listener.onError(
            CallStatus.INVALID_ARGUMENT
                .withDescription("Failed to execute statement (invalid syntax): " + e)
                .toRuntimeException());
      } catch (SQLException e) {
        listener.onError(
            CallStatus.INTERNAL
                .withDescription("Failed to execute statement: " + e)
                .toRuntimeException());
      } finally {
        clearLogCtx();
      }
    };
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(
      CommandPreparedStatementUpdate command,
      CallContext context,
      FlightStream flightStream,
      StreamListener<PutResult> listener) {
    return () -> {
      helper.setLogCtxFromCallContext("accept_put_prepared_statement_update", context);
      ByteString handle = command.getPreparedStatementHandle();
      log.info(
          "Accepting prepared statement for update query with handle = {}", handle.toStringUtf8());
      PreparedStatement statement = cache.getStatement(handle);

      if (statement == null) {
        listener.onError(
            CallStatus.NOT_FOUND
                .withDescription("Prepared statement does not exist")
                .toRuntimeException());
        clearLogCtx();
        return;
      }
      try (final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator(
              command.getPreparedStatementHandle().toStringUtf8(), 0, Long.MAX_VALUE)) {

        while (flightStream.next()) {
          final VectorSchemaRoot root = flightStream.getRoot();

          final int rowCount = root.getRowCount();
          final int recordCount;

          if (rowCount == 0) {
            statement.execute();
            recordCount = statement.getUpdateCount();
          } else {
            final JdbcParameterBinder binder =
                JdbcParameterBinder.builder(statement, root).bindAll().build();
            while (binder.next()) {
              statement.addBatch();
            }
            final int[] recordCounts = statement.executeBatch();
            recordCount = Arrays.stream(recordCounts).sum();
          }

          final DoPutUpdateResult build =
              DoPutUpdateResult.newBuilder().setRecordCount(recordCount).build();

          try (final ArrowBuf buffer = childAllocator.buffer(build.getSerializedSize())) {
            buffer.writeBytes(build.toByteArray());
            listener.onNext(PutResult.metadata(buffer));
          }
        }
      } catch (SQLException e) {
        listener.onError(
            CallStatus.INTERNAL
                .withDescription("Failed to execute update: " + e)
                .toRuntimeException());
        return;
      } finally {
        clearLogCtx();
      }
      listener.onCompleted();
    };
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(
      CommandPreparedStatementQuery command,
      CallContext context,
      FlightStream flightStream,
      StreamListener<PutResult> listener) {

    return () -> {
      helper.setLogCtxFromCallContext("accept_put_prepared_statement_query", context);
      ByteString handle = command.getPreparedStatementHandle();
      log.info(
          "Accepting parameter values for prepared statement with handle = {}",
          handle.toStringUtf8());
      PreparedStatement statement = cache.getStatement(command.getPreparedStatementHandle());

      if (statement == null) {
        listener.onError(
            CallStatus.NOT_FOUND
                .withDescription("Prepared statement does not exist")
                .toRuntimeException());
        clearLogCtx();
        return;
      }

      try {
        while (flightStream.next()) {
          final VectorSchemaRoot root = flightStream.getRoot();
          final JdbcParameterBinder binder =
              JdbcParameterBinder.builder(statement, root).bindAll().build();
          while (binder.next()) {
            // Do not execute() - will be done in a getStream call
          }
        }

      } catch (SQLException e) {
        listener.onError(
            CallStatus.INTERNAL
                .withDescription("Failed to bind parameters: " + e.getMessage())
                .withCause(e)
                .toRuntimeException());
        return;
      } finally {
        clearLogCtx();
      }

      listener.onCompleted();
    };
  }

  @Override
  public FlightInfo getFlightInfoSqlInfo(
      CommandGetSqlInfo command, CallContext context, FlightDescriptor descriptor) {
    helper.setLogCtxFromCallContext("get_flight_info_sql_info", context);
    log.info("Getting flight info for sql info codes: {}", command.getInfoList());
    clearLogCtx();
    return helper.getFlightInfoForSchema(command, descriptor, Schemas.GET_SQL_INFO_SCHEMA);
  }

  @Override
  public void getStreamSqlInfo(
      CommandGetSqlInfo command, CallContext context, ServerStreamListener listener) {
    helper.setLogCtxFromCallContext("get_stream_sql_info", context);
    log.info("Streaming sql info data for codes: {}", command.getInfoList());
    clearLogCtx();
    this.sqlInfoBuilder.send(command.getInfoList(), listener);
  }

  @Override
  public FlightInfo getFlightInfoTypeInfo(
      CommandGetXdbcTypeInfo command, CallContext context, FlightDescriptor descriptor) {
    helper.setLogCtxFromCallContext("get_flight_info_type_info", context);
    log.info("Getting flight info for type info code of `{}`", command.getDataType());
    clearLogCtx();
    return helper.getFlightInfoForSchema(command, descriptor, Schemas.GET_TYPE_INFO_SCHEMA);
  }

  @Override
  public void getStreamTypeInfo(
      CommandGetXdbcTypeInfo command, CallContext context, ServerStreamListener listener) {
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_stream_type_info", user.left);
    log.info("Streaming type info data for code of `{}`", command.getDataType());

    try (final Connection connection = trinoSource.getConnection(user.left, user.right);
        final ResultSet typeInfo = connection.getMetaData().getTypeInfo();
        final BufferAllocator childAllocator =
            rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE);
        final VectorSchemaRoot root = helper.getTypeInfoRoot(command, typeInfo, childAllocator)) {
      listener.start(root);
      listener.putNext();
      root.clear();
    } catch (SQLException e) {
      log.error(format("Failed to getStreamCatalogs: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
      clearLogCtx();
    }
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(
      CommandGetCatalogs command, CallContext context, FlightDescriptor descriptor) {
    helper.setLogCtxFromCallContext("get_flight_info_catalogs", context);
    log.info("Getting flight info for catalogs.");
    clearLogCtx();
    return helper.getFlightInfoForSchema(command, descriptor, Schemas.GET_CATALOGS_SCHEMA);
  }

  @Override
  public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_stream_catalogs", user.left);
    log.info("Streaming catalogs data.");
    try (final Connection connection = trinoSource.getConnection(user.left, user.right);
        final ResultSet catalogs = connection.getMetaData().getCatalogs();
        final BufferAllocator childAllocator =
            rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE);
        final VectorSchemaRoot root = helper.getCatalogsRoot(catalogs, childAllocator)) {
      listener.start(root);
      listener.putNext();
      root.clear();
    } catch (SQLException e) {
      log.error(format("Failed to getStreamCatalogs: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
      clearLogCtx();
    }
  }

  @Override
  public FlightInfo getFlightInfoSchemas(
      CommandGetDbSchemas command, CallContext context, FlightDescriptor descriptor) {
    helper.setLogCtxFromCallContext("get_flight_info_schemas", context);
    log.info("Getting flight info for schemas.");
    clearLogCtx();
    return helper.getFlightInfoForSchema(command, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
  }

  @Override
  public void getStreamSchemas(
      CommandGetDbSchemas command, CallContext context, ServerStreamListener listener) {
    final String catalog = command.hasCatalog() ? command.getCatalog() : null;
    final String schemaFilterPattern =
        command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_stream_schemas", user.left);
    log.info("Streaming schemas data.");

    try (final Connection connection = trinoSource.getConnection(user.left, user.right);
        final ResultSet schemas =
            connection.getMetaData().getSchemas(catalog, schemaFilterPattern);
        final BufferAllocator childAllocator =
            rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE);
        final VectorSchemaRoot root = helper.getSchemasRoot(schemas, childAllocator)) {
      listener.start(root);
      listener.putNext();
      root.clear();
    } catch (SQLException e) {
      log.error(format("Failed to getStreamSchemas: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
      clearLogCtx();
    }
  }

  @Override
  public FlightInfo getFlightInfoTables(
      CommandGetTables command, CallContext context, FlightDescriptor descriptor) {
    helper.setLogCtxFromCallContext("get_flight_info_tables", context);
    log.info("Getting flight info for tables.");
    clearLogCtx();

    Schema schemaToUse =
        !command.getIncludeSchema()
            ? Schemas.GET_TABLES_SCHEMA_NO_SCHEMA
            : Schemas.GET_TABLES_SCHEMA;
    return helper.getFlightInfoForSchema(command, descriptor, schemaToUse);
  }

  @Override
  public void getStreamTables(
      CommandGetTables command, CallContext context, ServerStreamListener listener) {
    final String catalog = command.hasCatalog() ? command.getCatalog() : null;
    final String schemaFilterPattern =
        command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
    final String tableFilterPattern =
        command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern() : null;

    final ProtocolStringList protocolStringList = command.getTableTypesList();
    final int protocolSize = protocolStringList.size();
    final String[] tableTypes =
        protocolSize == 0 ? null : protocolStringList.toArray(new String[protocolSize]);
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_stream_tables", user.left);
    log.info("Streaming tables data.");

    try (final Connection connection = trinoSource.getConnection(user.left, user.right);
        final BufferAllocator childAllocator =
            rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE);
        final VectorSchemaRoot root =
            helper.getTablesRoot(
                connection.getMetaData(),
                childAllocator,
                command.getIncludeSchema(),
                catalog,
                schemaFilterPattern,
                tableFilterPattern,
                tableTypes)) {
      listener.start(root);
      listener.putNext();
      root.clear();
    } catch (SQLException | IOException e) {
      log.error(format("Failed to getStreamTables: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
      clearLogCtx();
    }
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(
      CommandGetTableTypes command, CallContext context, FlightDescriptor descriptor) {
    helper.setLogCtxFromCallContext("get_flight_info_tables_types", context);
    log.info("Getting flight info for tables types.");
    clearLogCtx();
    return helper.getFlightInfoForSchema(command, descriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
  }

  @Override
  public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_stream_table_types", user.left);
    log.info("Streaming table types data.");

    try (final Connection connection = trinoSource.getConnection(user.left, user.right);
        final ResultSet tableTypes = connection.getMetaData().getTableTypes();
        final BufferAllocator childAllocator =
            rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE);
        final VectorSchemaRoot root = helper.getTableTypesRoot(tableTypes, childAllocator)) {
      listener.start(root);
      listener.putNext();
      root.clear();
    } catch (SQLException e) {
      log.error(format("Failed to getStreamTableTypes: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
      clearLogCtx();
    }
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(
      CommandGetPrimaryKeys command, CallContext context, FlightDescriptor descriptor) {
    helper.setLogCtxFromCallContext("get_flight_info_primary_keys", context);
    log.info("Getting flight info for primary keys.");
    clearLogCtx();
    return helper.getFlightInfoForSchema(command, descriptor, Schemas.GET_PRIMARY_KEYS_SCHEMA);
  }

  @Override
  public void getStreamPrimaryKeys(
      CommandGetPrimaryKeys command, CallContext context, ServerStreamListener listener) {
    final String catalog = command.hasCatalog() ? command.getCatalog() : null;
    final String schema = command.hasDbSchema() ? command.getDbSchema() : null;
    final String table = command.getTable();
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_stream_primary_keys", user.left);
    log.info("Streaming primary keys data.");

    try (final Connection connection = trinoSource.getConnection(user.left, user.right);
        final ResultSet primaryKeys =
            connection.getMetaData().getPrimaryKeys(catalog, schema, table);
        final BufferAllocator childAllocator =
            rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE);
        final VectorSchemaRoot root = helper.getPrimaryKeysRoot(primaryKeys, childAllocator)) {
      listener.start(root);
      listener.putNext();
      root.clear();
    } catch (SQLException e) {
      listener.error(e);
    } finally {
      listener.completed();
      clearLogCtx();
    }
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(
      CommandGetExportedKeys command, CallContext context, FlightDescriptor descriptor) {
    helper.setLogCtxFromCallContext("get_flight_info_exported_keys", context);
    log.info("Getting flight info for exported keys.");
    clearLogCtx();
    return helper.getFlightInfoForSchema(command, descriptor, Schemas.GET_EXPORTED_KEYS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(
      CommandGetImportedKeys command, CallContext context, FlightDescriptor descriptor) {
    helper.setLogCtxFromCallContext("get_flight_info_imported_keys", context);
    log.info("Getting flight info for imported keys.");
    clearLogCtx();
    return helper.getFlightInfoForSchema(command, descriptor, Schemas.GET_IMPORTED_KEYS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoCrossReference(
      CommandGetCrossReference command, CallContext context, FlightDescriptor descriptor) {
    helper.setLogCtxFromCallContext("get_flight_info_cross_reference", context);
    log.info("Getting flight info for cross references.");
    clearLogCtx();
    return helper.getFlightInfoForSchema(command, descriptor, Schemas.GET_CROSS_REFERENCE_SCHEMA);
  }

  @Override
  public void getStreamExportedKeys(
      CommandGetExportedKeys command, CallContext context, ServerStreamListener listener) {
    String catalog = command.hasCatalog() ? command.getCatalog() : null;
    String schema = command.hasDbSchema() ? command.getDbSchema() : null;
    String table = command.getTable();
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_stream_exported_keys", user.left);
    log.info("Streaming exported keys data.");
    try (final Connection connection = trinoSource.getConnection(user.left, user.right);
        final ResultSet keys = connection.getMetaData().getExportedKeys(catalog, schema, table);
        final BufferAllocator childAllocator =
            rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE);
        final VectorSchemaRoot root = helper.getKeysRoot(keys, childAllocator)) {
      listener.start(root);
      listener.putNext();
      root.clear();
    } catch (SQLException e) {
      listener.error(e);
    } finally {
      listener.completed();
      clearLogCtx();
    }
  }

  @Override
  public void getStreamImportedKeys(
      CommandGetImportedKeys command, CallContext context, ServerStreamListener listener) {
    String catalog = command.hasCatalog() ? command.getCatalog() : null;
    String schema = command.hasDbSchema() ? command.getDbSchema() : null;
    String table = command.getTable();
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_stream_imported_keys", user.left);
    log.info("Streaming imported keys data.");

    try (final Connection connection = trinoSource.getConnection(user.left, user.right);
        final ResultSet keys = connection.getMetaData().getImportedKeys(catalog, schema, table);
        final BufferAllocator childAllocator =
            rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE);
        final VectorSchemaRoot root = helper.getKeysRoot(keys, childAllocator)) {
      listener.start(root);
      listener.putNext();
      root.clear();
    } catch (final SQLException e) {
      listener.error(e);
    } finally {
      listener.completed();
      clearLogCtx();
    }
  }

  @Override
  public void getStreamCrossReference(
      CommandGetCrossReference command, CallContext context, ServerStreamListener listener) {
    final String pkCatalog = command.hasPkCatalog() ? command.getPkCatalog() : null;
    final String pkSchema = command.hasPkDbSchema() ? command.getPkDbSchema() : null;
    final String fkCatalog = command.hasFkCatalog() ? command.getFkCatalog() : null;
    final String fkSchema = command.hasFkDbSchema() ? command.getFkDbSchema() : null;
    final String pkTable = command.getPkTable();
    final String fkTable = command.getFkTable();
    ImmutablePair<String, String> user = helper.decodePeerIdentity(context.peerIdentity());
    setLogCtx("get_stream_cross_reference", user.left);
    log.info("Streaming cross references data.");

    try (final Connection connection = trinoSource.getConnection(user.left, user.right);
        final ResultSet keys =
            connection
                .getMetaData()
                .getCrossReference(pkCatalog, pkSchema, pkTable, fkCatalog, fkSchema, fkTable);
        final BufferAllocator childAllocator =
            rootAllocator.newChildAllocator(randomUUID().toString(), 0, Long.MAX_VALUE);
        final VectorSchemaRoot root = helper.getKeysRoot(keys, childAllocator)) {
      listener.start(root);
      listener.putNext();
      root.clear();
    } catch (final SQLException e) {
      listener.error(e);
    } finally {
      listener.completed();
      clearLogCtx();
    }
  }

  @Override
  public void close() throws Exception {
    log.info("Closing producer");
    try {
      AutoCloseables.close(trinoSource, rootAllocator, cache);
    } catch (Throwable t) {
      log.error(format("Failed to close resources: <%s>", t.getMessage()), t);
    }
  }

  @Override
  public void listFlights(
      CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
    helper.setLogCtxFromCallContext("list_flights", callContext);
    log.error("UNIMPLEMENTED");
    clearLogCtx();
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }
}
