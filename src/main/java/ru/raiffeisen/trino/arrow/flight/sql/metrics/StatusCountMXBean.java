package ru.raiffeisen.trino.arrow.flight.sql.metrics;

import org.apache.arrow.flight.FlightStatusCode;

import java.util.Map;

public interface StatusCountMXBean {

  Map<Status, Long> getCount();

  long getTotalCount();

  class Status implements Comparable<Status> {
    private final FlightStatusCode code;

    public Status(FlightStatusCode code) {
      this.code = code;
    }

    public String getCode() {
      return this.code.toString();
    }

    public boolean equals(final Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      Status status = (Status) other;
      return code.equals(status.code);
    }

    public int hashCode() {
      return this.code.hashCode();
    }

    @Override
    public int compareTo(Status o) {
      return this.code.compareTo(o.code);
    }
  }
}
