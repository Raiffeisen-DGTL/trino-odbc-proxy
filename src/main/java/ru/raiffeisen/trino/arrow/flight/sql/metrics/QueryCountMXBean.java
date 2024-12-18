package ru.raiffeisen.trino.arrow.flight.sql.metrics;

import java.util.Map;

public interface QueryCountMXBean {
  Map<String, Long> getCountByUser();

  Map<QuerySizeGroup, Long> getCountBySize();

  long getTotalCount();
}
