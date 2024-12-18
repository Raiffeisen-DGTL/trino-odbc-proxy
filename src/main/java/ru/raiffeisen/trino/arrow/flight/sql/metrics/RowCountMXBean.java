package ru.raiffeisen.trino.arrow.flight.sql.metrics;

import java.util.Map;

public interface RowCountMXBean {
  long getTotalCount();

  long getAvgRowsPerQuery();

  double getStreamingRate();

  Map<String, Long> getRowsPerUser();

  class StreamingRateRecord {
    private long numRows;
    private long streamingTime;

    public StreamingRateRecord(long numRows, long streamingTime) {
      this.numRows = numRows;
      this.streamingTime = streamingTime;
    }

    public StreamingRateRecord() {
      this(0, 0);
    }

    public double getStreamingRate() {
      return numRows == 0 ? 0d : (double) numRows / streamingTime;
    }

    public StreamingRateRecord increment(long numRows, long streamingTime) {
      this.numRows += numRows;
      this.streamingTime += streamingTime;
      return this;
    }

    public static StreamingRateRecord merge(
        StreamingRateRecord record1, StreamingRateRecord record2) {
      return record1.increment(record2.numRows, record2.streamingTime);
    }
  }
}
