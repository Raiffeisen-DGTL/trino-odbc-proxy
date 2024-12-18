package ru.raiffeisen.trino.arrow.flight.sql.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class RowCount implements RowCountMXBean {
  
  private final AtomicLong totalRows;
  private final AtomicLong totalQueries;
  private final AtomicLong totalStreamingTime;
  private final ConcurrentHashMap<String, Long> rowsPerUser;
  private final ConcurrentLinkedQueue<StreamingRateRecord> rowsPerLast10Queries;
  
  public RowCount() {
    totalRows = new AtomicLong(0);
    totalQueries = new AtomicLong(0);
    totalStreamingTime = new AtomicLong(0);
    rowsPerUser = new ConcurrentHashMap<>();
    rowsPerLast10Queries = new ConcurrentLinkedQueue<>();
  }
  
  public void increment(String user, int numRows, long streamingTime) {
    totalRows.addAndGet(numRows);
    totalStreamingTime.addAndGet(streamingTime);
    totalQueries.incrementAndGet();
    rowsPerUser.compute(user, (k, v) -> v == null ? numRows : v + numRows);
    
    rowsPerLast10Queries.offer(new StreamingRateRecord(numRows, streamingTime));
    
    if (rowsPerLast10Queries.size() > 10) {
      rowsPerLast10Queries.poll();
    }
  }
  
  @Override
  public long getTotalCount() {
    return totalRows.get();
  }

  @Override
  public long getAvgRowsPerQuery() {
    long rows = totalRows.get();
    return rows == 0 ? 0L : rows / totalQueries.get();
  }

  @Override
  public double getStreamingRate() {
    return rowsPerLast10Queries.stream()
        .reduce(new StreamingRateRecord(0L, 0L), StreamingRateRecord::merge)
        .getStreamingRate();
  }

  @Override
  public Map<String, Long> getRowsPerUser() {
    return rowsPerUser;
  }
}
