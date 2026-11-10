package io.github.vinayakkothari.throttle.client;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Stops calling a backend that keeps failing, so a dead limiter costs each request nothing
 * instead of a full timeout.
 *
 * <pre>
 *   CLOSED     calls go through; N failures in a row open the breaker
 *   OPEN       calls fail fast for a cool-down period
 *   HALF_OPEN  one trial call; success closes the breaker, failure opens it again
 * </pre>
 */
public final class CircuitBreaker {
  public enum State {
    CLOSED,
    OPEN,
    HALF_OPEN
  }

  private final int failureThreshold;
  private final long openNanos;
  private final LongSupplier clock;
  private final AtomicInteger failures = new AtomicInteger();
  private final AtomicLong openedAt = new AtomicLong(-1);
  private final AtomicBoolean probing = new AtomicBoolean();

  public CircuitBreaker(int failureThreshold, Duration openFor) {
    this(failureThreshold, openFor, System::nanoTime);
  }

  CircuitBreaker(int failureThreshold, Duration openFor, LongSupplier clock) {
    if (failureThreshold < 1) throw new IllegalArgumentException("threshold must be at least 1");
    this.failureThreshold = failureThreshold;
    this.openNanos = openFor.toNanos();
    this.clock = clock;
  }

  /** True if this call may go to the backend. In half-open state only one caller gets true. */
  public boolean tryPass() {
    long opened = openedAt.get();
    if (opened < 0) return true;
    if (clock.getAsLong() - opened < openNanos) return false;
    return probing.compareAndSet(false, true);
  }

  public void onSuccess() {
    failures.set(0);
    openedAt.set(-1);
    probing.set(false);
  }

  public void onFailure() {
    if (probing.getAndSet(false)) {
      openedAt.set(clock.getAsLong()); // the trial call failed: back to open
      return;
    }
    if (failures.incrementAndGet() >= failureThreshold) {
      openedAt.compareAndSet(-1, clock.getAsLong());
    }
  }

  public State state() {
    long opened = openedAt.get();
    if (opened < 0) return State.CLOSED;
    return clock.getAsLong() - opened < openNanos ? State.OPEN : State.HALF_OPEN;
  }
}
