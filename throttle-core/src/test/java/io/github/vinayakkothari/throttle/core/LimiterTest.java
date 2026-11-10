package io.github.vinayakkothari.throttle.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vinayakkothari.throttle.core.redis.RedisClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LimiterTest {
  private RedisClient redis;
  private Limiter limiter;

  @BeforeEach
  void connect() {
    redis = TestRedis.connect();
    limiter = new Limiter(redis, TestRedis.prefix());
  }

  @AfterEach
  void close() {
    if (redis != null) redis.close();
  }

  private static Rule rule(Algorithm a, long limit, Duration period) {
    return Rule.of("r", a, limit, period);
  }

  @Test
  void everyAlgorithmAllowsTheLimitThenDenies() {
    for (Algorithm a : Algorithm.values()) {
      Rule r = rule(a, 5, Duration.ofMinutes(1));
      for (int i = 0; i < 5; i++) {
        Decision d = limiter.check(r, "k", 1);
        assertTrue(d.allowed(), a + " request " + i);
        assertEquals(4 - i, d.remaining(), a + " remaining");
      }
      Decision denied = limiter.check(r, "k", 1);
      assertFalse(denied.allowed(), a + " should deny the sixth");
      assertTrue(denied.retryAfterMillis() > 0, a + " should say when to retry");
      assertEquals(5, denied.limit());
    }
  }

  @Test
  void requestLargerThanTheLimitCanNeverFit() {
    for (Algorithm a : Algorithm.values()) {
      Decision d = limiter.check(rule(a, 5, Duration.ofSeconds(1)), "big", 6);
      assertFalse(d.allowed());
      assertEquals(-1, d.retryAfterMillis(), a.toString());
    }
  }

  @Test
  void partialAcquireTakesWhatIsLeft() {
    for (Algorithm a : Algorithm.values()) {
      Rule r = rule(a, 10, Duration.ofMinutes(1));
      assertEquals(3, limiter.check(r, "p", 3).granted());
      Decision lease = limiter.acquire(r, "p", 50, true);
      assertEquals(7, lease.granted(), a.toString());
      Decision empty = limiter.acquire(r, "p", 50, true);
      assertEquals(0, empty.granted());
      assertFalse(empty.allowed());
    }
  }

  @Test
  void allOrNothingDoesNotTakeAPartialAmount() {
    Rule r = rule(Algorithm.TOKEN_BUCKET, 10, Duration.ofMinutes(1));
    limiter.check(r, "a", 8);
    assertFalse(limiter.check(r, "a", 3).allowed());
    assertTrue(limiter.check(r, "a", 2).allowed(), "the failed call must not have used tokens");
  }

  @Test
  void tokenBucketRefillsAtItsRate() throws Exception {
    Rule r = Rule.tokenBucket("tb", 20, Duration.ofSeconds(1), 2); // 1 token every 50 ms
    assertTrue(limiter.check(r, "k", 2).allowed());
    Decision empty = limiter.check(r, "k", 1);
    assertFalse(empty.allowed());
    assertTrue(empty.retryAfterMillis() <= 50, "retry " + empty.retryAfterMillis());
    Thread.sleep(empty.retryAfterMillis() + 10);
    assertTrue(limiter.check(r, "k", 1).allowed(), "a token should have refilled");
  }

  @Test
  void slidingLogForgetsRequestsOnceTheyLeaveTheWindow() throws Exception {
    Rule r = rule(Algorithm.SLIDING_WINDOW_LOG, 3, Duration.ofMillis(300));
    for (int i = 0; i < 3; i++) assertTrue(limiter.check(r, "k", 1).allowed());
    Decision denied = limiter.check(r, "k", 1);
    assertFalse(denied.allowed());
    assertTrue(denied.retryAfterMillis() > 0 && denied.retryAfterMillis() <= 300);
    Thread.sleep(denied.retryAfterMillis() + 20);
    assertTrue(limiter.check(r, "k", 1).allowed());
  }

  @Test
  void slidingCounterRetryAfterIsUsable() throws Exception {
    Rule r = rule(Algorithm.SLIDING_WINDOW_COUNTER, 4, Duration.ofMillis(400));
    for (int i = 0; i < 4; i++) assertTrue(limiter.check(r, "k", 1).allowed());
    Decision denied = limiter.check(r, "k", 1);
    assertFalse(denied.allowed());
    assertTrue(denied.retryAfterMillis() <= 800, "retry " + denied.retryAfterMillis());
    Thread.sleep(denied.retryAfterMillis() + 20);
    assertTrue(limiter.check(r, "k", 1).allowed(), "should be allowed once retry has passed");
  }

  @Test
  void overridesGiveAKeyItsOwnLimit() {
    Rule r = Rule.tokenBucket("api", 2, Duration.ofMinutes(1), 2).withOverrides(Map.of("vip", 5L));
    assertEquals(2, limiter.acquire(r, "free", 10, true).granted());
    assertEquals(5, limiter.acquire(r, "vip", 10, true).granted());
  }

  @Test
  void keysAreIndependent() {
    Rule r = rule(Algorithm.TOKEN_BUCKET, 1, Duration.ofMinutes(1));
    assertTrue(limiter.check(r, "a", 1).allowed());
    assertTrue(limiter.check(r, "b", 1).allowed());
    assertFalse(limiter.check(r, "a", 1).allowed());
  }

  @Test
  void switchingAlgorithmUsesAFreshKey() {
    Rule tb = rule(Algorithm.TOKEN_BUCKET, 1, Duration.ofMinutes(1));
    Rule log = rule(Algorithm.SLIDING_WINDOW_LOG, 1, Duration.ofMinutes(1));
    assertNotEquals(limiter.redisKey(tb, "k"), limiter.redisKey(log, "k"));
    limiter.check(tb, "k", 1);
    assertTrue(limiter.check(log, "k", 1).allowed(), "no WRONGTYPE error, no shared state");
  }

  @Test
  void rejectsBadArguments() {
    Rule r = rule(Algorithm.TOKEN_BUCKET, 1, Duration.ofSeconds(1));
    assertThrows(IllegalArgumentException.class, () -> limiter.check(r, "", 1));
    assertThrows(IllegalArgumentException.class, () -> limiter.check(r, "k".repeat(300), 1));
    assertThrows(IllegalArgumentException.class, () -> limiter.check(r, "k", 0));
  }

  /** The point of the Lua scripts: many callers at once can never take more than the limit. */
  @Test
  void neverExceedsTheLimitUnderConcurrency() throws Exception {
    for (Algorithm a : Algorithm.values()) {
      Rule r = rule(a, 500, Duration.ofHours(1)); // no refill during the test
      AtomicLong allowed = new AtomicLong();
      try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
        List<Future<?>> tasks = new ArrayList<>();
        for (int t = 0; t < 50; t++) {
          tasks.add(
              pool.submit(
                  () -> {
                    for (int i = 0; i < 20; i++) {
                      if (limiter.check(r, "hot", 1).allowed()) allowed.incrementAndGet();
                    }
                  }));
        }
        for (Future<?> f : tasks) f.get();
      }
      assertEquals(500, allowed.get(), a + ": 1000 attempts against a limit of 500");
    }
  }
}
