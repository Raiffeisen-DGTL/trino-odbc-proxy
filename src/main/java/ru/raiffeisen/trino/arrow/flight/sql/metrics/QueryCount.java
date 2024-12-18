package ru.raiffeisen.trino.arrow.flight.sql.metrics;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class QueryCount implements QueryCountMXBean {

  private final ConcurrentHashMap<String, Long> queriesByUser;
  private final ConcurrentHashMap<QuerySizeGroup, Long> queriesBySize;
  
  public QueryCount() {
    queriesBySize = new ConcurrentHashMap<>();
    queriesByUser = new ConcurrentHashMap<>();
  }

  @Override
  public Map<String, Long> getCountByUser() {
    return queriesByUser;
  }

  @Override
  public Map<QuerySizeGroup, Long> getCountBySize() {
    return queriesBySize;
  }

  @Override
  public long getTotalCount() {
    return queriesByUser.values().stream().reduce(0L, Long::sum);
  }

  public void increment(String user, int numRows) {
    Objects.requireNonNull(user);
    QuerySizeGroup qsg = new QuerySizeGroup(numRows);
    queriesByUser.compute(user, (k, v) -> v == null ? 1 : v + 1);
    queriesBySize.compute(qsg, (k, v) -> v == null ? 1 : v + 1);
  }
}
