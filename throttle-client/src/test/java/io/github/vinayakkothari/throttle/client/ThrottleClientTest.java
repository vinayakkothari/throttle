package io.github.vinayakkothari.throttle.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vinayakkothari.throttle.core.Decision;
import io.github.vinayakkothari.throttle.core.FailMode;
import io.github.vinayakkothari.throttle.core.Rule;
import io.github.vinayakkothari.throttle.core.UnknownRuleException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ThrottleClientTest {
  private final AtomicLong now = new AtomicLong(1_000_000_000L);

  private void advance(Duration d) {
    now.addAndGet(d.toNanos());
  }

  private ThrottleClient.Builder client(FakeBackend backend) {
    return ThrottleClient.builder(backend).clock(now::get);
  }

  private static List<Rule> rules(Rule... r) {
    return List.of(r);
  }

  @Test
  void withoutLeasingEveryDecisionCallsTheBackend() {
    FakeBackend backend = new FakeBackend(1000, List.of());
    try (ThrottleClient c = client(backend).build()) {
      for (int i = 0; i < 100; i++) assertTrue(c.tryAcquire("api", "k").allowed());
      assertEquals(100, backend.calls.get());
      assertEquals(1000.0, c.stats().callsPerThousand());
    }
  }

  @Test
  void leasingServesMostDecisionsLocally() {
    FakeBackend backend = new FakeBackend(10_000, List.of());
    try (ThrottleClient c = client(backend).leaseSize(50).build()) {
      for (int i = 0; i < 1000; i++) assertTrue(c.tryAcquire("api", "k").allowed());
      assertEquals(20, backend.calls.get(), "1000 decisions in leases of 50");
      assertEquals(980, c.stats().servedFromLease());
    }
  }

  @Test
  void leasedTokensExpire() {
    FakeBackend backend = new FakeBackend(10_000, List.of());
    try (ThrottleClient c = client(backend).leaseSize(50).leaseTtl(Duration.ofMillis(100)).build()) {
      c.tryAcquire("api", "k"); // leases 50, spends 1
      advance(Duration.ofMillis(150));
      c.tryAcquire("api", "k"); // the 49 left have expired, lease again
      assertEquals(2, backend.calls.get());
      assertEquals(49, c.stats().expiredTokens());
    }
  }

  @Test
  void neverHandsOutMoreThanTheBackendGranted() throws Exception {
    FakeBackend backend = new FakeBackend(500, List.of());
    AtomicLong allowed = new AtomicLong();
    try (ThrottleClient c = client(backend).leaseSize(30).build();
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<?>> tasks = new ArrayList<>();
      for (int t = 0; t < 40; t++) {
        tasks.add(pool.submit(() -> {
          for (int i = 0; i < 25; i++) if (c.tryAcquire("api", "hot").allowed()) allowed.incrementAndGet();
        }));
      }
      for (Future<?> f : tasks) f.get();
    }
    assertEquals(500, allowed.get(), "1000 attempts, budget 500, leases of 30");
  }

  @Test
  void negativeCacheAnswersDenialsLocally() {
    FakeBackend backend = new FakeBackend(2, List.of());
    backend.retryAfter = 500;
    try (ThrottleClient c = client(backend).build()) {
      c.tryAcquire("api", "k");
      c.tryAcquire("api", "k");
      Decision denied = c.tryAcquire("api", "k");
      assertFalse(denied.allowed());
      int callsAfterFirstDenial = backend.calls.get();
      for (int i = 0; i < 100; i++) assertFalse(c.tryAcquire("api", "k").allowed());
      assertEquals(callsAfterFirstDenial, backend.calls.get(), "no backend calls while known empty");
      assertEquals(100, c.stats().negativeCacheHits());

      advance(Duration.ofMillis(501));
      c.tryAcquire("api", "k");
      assertEquals(callsAfterFirstDenial + 1, backend.calls.get(), "asks again after retry time");
    }
  }

  @Test
  void negativeCacheIsCappedSoRaisedLimitsAreNoticed() {
    FakeBackend backend = new FakeBackend(0, List.of());
    backend.retryAfter = 60_000;
    try (ThrottleClient c = client(backend).negativeCacheMax(Duration.ofMillis(200)).build()) {
      c.tryAcquire("api", "k");
      advance(Duration.ofMillis(201));
      c.tryAcquire("api", "k");
      assertEquals(2, backend.calls.get());
    }
  }

  @Test
  void circuitBreakerStopsCallingADeadBackendAndRecovers() {
    FakeBackend backend = new FakeBackend(1000, List.of());
    try (ThrottleClient c =
        client(backend).circuitBreaker(3, Duration.ofSeconds(1)).fallback(Fallback.allow()).build()) {
      backend.down = true;
      for (int i = 0; i < 10; i++) {
        Decision d = c.tryAcquire("api", "k");
        assertTrue(d.allowed() && d.degraded());
      }
      assertEquals(3, backend.calls.get(), "breaker opened after 3 failures");
      assertEquals(CircuitBreaker.State.OPEN, c.stats().breaker());

      backend.down = false;
      advance(Duration.ofMillis(1001));
      assertEquals(CircuitBreaker.State.HALF_OPEN, c.stats().breaker());
      Decision probe = c.tryAcquire("api", "k");
      assertFalse(probe.degraded());
      assertEquals(CircuitBreaker.State.CLOSED, c.stats().breaker());
    }
  }

  @Test
  void ruleDefaultFallbackFollowsEachRulesFailMode() throws Exception {
    Rule open = Rule.tokenBucket("open", 10, Duration.ofSeconds(1), 10);
    Rule closed = Rule.tokenBucket("closed", 10, Duration.ofSeconds(1), 10).withFailMode(FailMode.CLOSED);
    FakeBackend backend = new FakeBackend(10, rules(open, closed));
    try (ThrottleClient c = client(backend).build()) {
      waitForRules(c, backend);
      backend.down = true;
      assertTrue(c.tryAcquire("open", "k").allowed());
      assertFalse(c.tryAcquire("closed", "k").allowed());
      assertTrue(c.tryAcquire("never-seen", "k").allowed(), "unknown rules fail open");
    }
  }

  @Test
  void localFallbackKeepsLimitingAtAShareOfTheRule() throws Exception {
    Rule rule = Rule.tokenBucket("api", 100, Duration.ofMinutes(1), 100);
    FakeBackend backend = new FakeBackend(100, rules(rule));
    try (ThrottleClient c = client(backend).fallback(Fallback.local(4)).build()) {
      waitForRules(c, backend);
      backend.down = true;
      int allowed = 0;
      for (int i = 0; i < 100; i++) if (c.tryAcquire("api", "k").allowed()) allowed++;
      assertEquals(25, allowed, "one of 4 servers gets a quarter of 100");
      assertEquals(100, c.stats().fallbacks());
    }
  }

  @Test
  void unknownRuleIsTheCallersErrorAndDoesNotTripTheBreaker() {
    FakeBackend backend = new FakeBackend(10, List.of());
    try (ThrottleClient c = client(backend).circuitBreaker(1, Duration.ofSeconds(1)).build()) {
      assertThrows(UnknownRuleException.class, () -> c.tryAcquire("missing", "k"));
      assertEquals(CircuitBreaker.State.CLOSED, c.stats().breaker());
    }
  }

  @Test
  void costLargerThanTheLeaseStillWorks() {
    FakeBackend backend = new FakeBackend(100, List.of());
    try (ThrottleClient c = client(backend).leaseSize(10).build()) {
      Decision d = c.tryAcquire("api", "k", 25);
      assertTrue(d.allowed());
      assertEquals(25, d.granted());
      assertEquals(75L, (long) backend.budget.get("k"));
    }
  }

  private static void waitForRules(ThrottleClient c, FakeBackend backend) throws Exception {
    // rules load on a background thread at startup; give it a moment
    Thread.sleep(100);
  }
}
