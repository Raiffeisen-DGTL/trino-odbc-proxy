package ru.raiffeisen.trino.arrow.flight.sql.producer;

import com.google.common.base.Preconditions;
import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.FlightProducer;

import java.util.concurrent.atomic.AtomicLong;

/** Implementation of back pressure strategy to wait for listener to be ready streaming data.
 * Key points:
 * <ul>
 *   <li>busy wait for listener to be ready with thread sleep for 1 millisecond;</li>
 *   <li>count total time the server has waited for listener (we will monitor this metric).</li>
 * </ul>
 */
public class BackpressureStrategyImpl implements BackpressureStrategy {
  private FlightProducer.ServerStreamListener listener;
  private final AtomicLong sleepTime = new AtomicLong(0);

  public long getSleepTime() {
    return sleepTime.get();
  }

  @Override
  public void register(FlightProducer.ServerStreamListener serverStreamListener) {
    this.listener = serverStreamListener;
  }

  @Override
  public WaitResult waitForListener(long timeout) {
    Preconditions.checkNotNull(listener);
    long remainingTimeout = timeout;
    while (!listener.isReady() && !listener.isCancelled()) {
      try {
        Thread.sleep(1L);
        sleepTime.addAndGet(1L);
        remainingTimeout -= 1L;
        if (remainingTimeout <= 0L) {
          return WaitResult.TIMEOUT;
        }
      } catch (InterruptedException ex) {
        return WaitResult.OTHER;
      }
    }
    if (listener.isReady()) {
      return WaitResult.READY;
    } else if (listener.isCancelled()) {
      return WaitResult.CANCELLED;
    }
    throw new RuntimeException("Invalid state when waiting for listener.");
  }
}
