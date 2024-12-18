package ru.raiffeisen.trino.arrow.flight.sql.producer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.ByteString;
import org.apache.arrow.util.AutoCloseables;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.UUID.randomUUID;

/**
 * Cache used to store prepared statements in memory between {@link TrinoFlightSQLProducer} cals.
 */
public final class FlightSQLProducerCache implements AutoCloseable {

  /**
   * Cache to store prepared statements in memory.
   */
  private final Cache<ByteString, StatementContext> statementCache;

  public FlightSQLProducerCache() {
    this.statementCache =
        CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .removalListener(new StatementRemovalListener())
            .build();
  }
  
  private ByteString getRandomHandle() {
    return copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Creates {@link StatementContext} from statement and query from which this statement was built
   * and puts it into a producer cache. Returns handle which by which this context is identified
   * inside cache.
   *
   * @param statement Statement to cache
   * @param query Query to cache
   * @return Handle identifying this statement context.
   */
  public ByteString put(PreparedStatement statement, String query) {
    final ByteString handle = getRandomHandle();
    statementCache.put(handle, new StatementContext(statement, query));
    return handle;
  }
  
  public PreparedStatement getStatement(ByteString handle) {
    final StatementContext ctx = statementCache.getIfPresent(handle);
    return ctx != null ? ctx.getStatement() : null;
  }
  
  public String getQuery(ByteString handle) {
    final StatementContext ctx = statementCache.getIfPresent(handle);
    return ctx != null ? ctx.getQuery() : null;
  }
  
  public void invalidate(ByteString handle) {
    statementCache.invalidate(handle);
  }
  
  /**
   * Context for prepared statement to be persisted in memory.
   */
  private static class StatementContext implements AutoCloseable {

    private final PreparedStatement statement;
    private final String query;

    public StatementContext(final PreparedStatement statement, final String query) {
      this.statement = Objects.requireNonNull(statement, "statement cannot be null.");
      this.query = query;
    }

    /**
     * Gets the statement wrapped by this {@link StatementContext}.
     *
     * @return the inner statement.
     */
    public PreparedStatement getStatement() {
      return statement;
    }

    /**
     * Gets the optional SQL query wrapped by this {@link StatementContext}.
     *
     * @return the SQL query if present; empty otherwise.
     */
    public String getQuery() {
      return query;
    }

    @Override
    public void close() throws Exception {
      Connection connection = statement.getConnection();
      AutoCloseables.close(statement, connection);
    }

    @Override
    public boolean equals(final Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof StatementContext that)) {
        return false;
      }
      return statement.equals(that.statement);
    }

    @Override
    public int hashCode() {
      return Objects.hash(statement);
    }
  }

  /**
   * Removal listener to invalidate cached prepared statements.
   */
  private static class StatementRemovalListener implements RemovalListener<ByteString, StatementContext> {
    @Override
    public void onRemoval(RemovalNotification<ByteString, StatementContext> notification) {
      try {
        AutoCloseables.close(notification.getValue());
      } catch (final Exception e) {
        // swallow
      }
    }
  }

  @Override
  public void close() throws Exception {
    this.statementCache.cleanUp();
  }
}
