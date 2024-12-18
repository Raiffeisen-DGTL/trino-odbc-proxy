package ru.raiffeisen.trino.arrow.flight.sql.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class BufferMemAllocation implements BufferMemAllocationMBean {

  private final AtomicLong memAllocated;

  public BufferMemAllocation() {
    this.memAllocated = new AtomicLong();
  }

  public void update(long allocated) {
    this.memAllocated.set(allocated);
  }

  @Override
  public long getAllocated() {
    return this.memAllocated.get();
  }
}
