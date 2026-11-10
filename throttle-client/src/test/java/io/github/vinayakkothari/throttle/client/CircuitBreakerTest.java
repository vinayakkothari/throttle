package io.github.vinayakkothari.throttle.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class CircuitBreakerTest {
  private final AtomicLong now = new AtomicLong();
  private final CircuitBreaker breaker = new CircuitBreaker(2, Duration.ofSeconds(1), now::get);

  @Test
  void opensAfterConsecutiveFailures() {
    breaker.onFailure();
    breaker.onSuccess(); // a success in between resets the count
    breaker.onFailure();
    assertEquals(CircuitBreaker.State.CLOSED, breaker.state());
    breaker.onFailure();
    assertEquals(CircuitBreaker.State.OPEN, breaker.state());
    assertFalse(breaker.tryPass());
  }

  @Test
  void halfOpenLetsExactlyOneTrialThrough() {
    breaker.onFailure();
    breaker.onFailure();
    now.addAndGet(Duration.ofSeconds(1).toNanos());
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.state());
    assertTrue(breaker.tryPass());
    assertFalse(breaker.tryPass(), "only one trial at a time");
  }

  @Test
  void failedTrialReopensSuccessfulTrialCloses() {
    breaker.onFailure();
    breaker.onFailure();
    now.addAndGet(Duration.ofSeconds(1).toNanos());
    assertTrue(breaker.tryPass());
    breaker.onFailure();
    assertEquals(CircuitBreaker.State.OPEN, breaker.state());

    now.addAndGet(Duration.ofSeconds(1).toNanos());
    assertTrue(breaker.tryPass());
    breaker.onSuccess();
    assertEquals(CircuitBreaker.State.CLOSED, breaker.state());
    assertTrue(breaker.tryPass());
  }
}
