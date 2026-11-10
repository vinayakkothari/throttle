package io.github.vinayakkothari.throttle.client;

import java.util.concurrent.locks.ReentrantLock;

/** An in-memory token bucket, used only while the backend is unreachable. */
final class LocalBucket {
  private final double capacity;
  private final double ratePerNano;
  private final ReentrantLock lock = new ReentrantLock();
  private double tokens;
  private long last;

  LocalBucket(double capacity, double ratePerNano, long now) {
    this.capacity = Math.max(1, capacity);
    this.ratePerNano = ratePerNano;
    this.tokens = this.capacity;
    this.last = now;
  }

  boolean tryTake(long cost, long now) {
    lock.lock();
    try {
      if (now > last) {
        tokens = Math.min(capacity, tokens + (now - last) * ratePerNano);
        last = now;
      }
      if (tokens >= cost) {
        tokens -= cost;
        return true;
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  long available() {
    return (long) tokens;
  }
}
