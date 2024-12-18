package ru.raiffeisen.trino.arrow.flight.sql.metrics;

import org.apache.arrow.flight.FlightStatusCode;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class StatusCount implements StatusCountMXBean {

  private final ConcurrentHashMap<Status, Long> statuses;

  public StatusCount() {
    statuses = new ConcurrentHashMap<>();
  }

  @Override
  public Map<Status, Long> getCount() {
    return statuses;
  }

  @Override
  public long getTotalCount() {
    return statuses.values().stream().reduce(0L, Long::sum);
  }

  public void increment(FlightStatusCode code) {
    Objects.requireNonNull(code);
    Status status = new Status(code);
    statuses.compute(status, (k, v) -> v == null ? 1 : v + 1);
  }
}
