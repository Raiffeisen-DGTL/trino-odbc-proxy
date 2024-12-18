package ru.raiffeisen.trino.arrow.flight.sql.metrics;

import java.util.Map;

public interface ListenerWaitMXBean {

  Map<QuerySizeGroup, Long> getWaitTime();

  Map<QuerySizeGroup, Double> getAvgWaitTime();

  long getTotalWaitTime();

  Double getTotalAvgWaitTime();

  class WaitTimeRecord {
    private long numRows;
    private long waitTime;

    public WaitTimeRecord(long numRows, long waitTime) {
      this.numRows = numRows;
      this.waitTime = waitTime;
    }

    public WaitTimeRecord() {
      this(0L, 0L);
    }

    public Long getWaitTime() {
      return this.waitTime;
    }

    public Double getAvgWaitTime() {
      return this.numRows == 0 ? 0d : (double) this.waitTime / this.numRows;
    }

    public WaitTimeRecord increment(long numRows, long waitTime) {
      this.numRows += numRows;
      this.waitTime += waitTime;
      return this;
    }

    public static WaitTimeRecord merge(WaitTimeRecord record1, WaitTimeRecord record2) {
      record1.increment(record2.numRows, record2.waitTime);
      return record1;
    }
  }
}
