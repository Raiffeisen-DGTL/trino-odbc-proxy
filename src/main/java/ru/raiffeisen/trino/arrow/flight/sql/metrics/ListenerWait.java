package ru.raiffeisen.trino.arrow.flight.sql.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ListenerWait implements ListenerWaitMXBean {

  private final ConcurrentHashMap<QuerySizeGroup, WaitTimeRecord> waitTimes;

  public ListenerWait() {
    waitTimes = new ConcurrentHashMap<>();
  }

  @Override
  public Map<QuerySizeGroup, Long> getWaitTime() {
    return waitTimes.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getWaitTime()));
  }

  @Override
  public Map<QuerySizeGroup, Double> getAvgWaitTime() {
    return waitTimes.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getAvgWaitTime()));
  }

  @Override
  public long getTotalWaitTime() {
    return waitTimes.values().stream().map(WaitTimeRecord::getWaitTime).reduce(0L, Long::sum);
  }

  @Override
  public Double getTotalAvgWaitTime() {
    return waitTimes.values().stream()
        .reduce(new WaitTimeRecord(0L, 0L), WaitTimeRecord::merge)
        .getAvgWaitTime();
  }

  public void increment(int numRows, Long waitTime) {
    QuerySizeGroup qsg = new QuerySizeGroup(numRows);
    WaitTimeRecord waitTimeRecord = new WaitTimeRecord((long) numRows, waitTime);
    waitTimes.compute(
        qsg, (k, v) -> v == null ? waitTimeRecord : WaitTimeRecord.merge(v, waitTimeRecord));
  }
}
