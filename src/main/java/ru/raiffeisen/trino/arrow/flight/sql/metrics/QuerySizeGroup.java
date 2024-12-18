package ru.raiffeisen.trino.arrow.flight.sql.metrics;

public class QuerySizeGroup implements Comparable<QuerySizeGroup> {

  private final int sizeGroup;

  public QuerySizeGroup(int numRows) {
    this.sizeGroup = (int) Math.log10(numRows);
  }

  public int getSizeGroup() {
    return this.sizeGroup;
  }

  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    QuerySizeGroup qsg = (QuerySizeGroup) other;
    return this.sizeGroup == qsg.getSizeGroup();
  }

  public int hashCode() {
    return Integer.hashCode(this.sizeGroup);
  }

  @Override
  public int compareTo(QuerySizeGroup o) {
    return Integer.compare(this.sizeGroup, o.sizeGroup);
  }
}
