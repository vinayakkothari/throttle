package io.github.vinayakkothari.throttle.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.vinayakkothari.throttle.core.redis.RedisClient;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RuleStoreTest {
  private RedisClient redis;
  private String prefix;
  private RuleStore writer;
  private RuleStore reader;

  @BeforeEach
  void setUp() {
    redis = TestRedis.connect();
    prefix = TestRedis.prefix();
    writer = new RuleStore(redis, prefix);
    reader = new RuleStore(redis, prefix).start(Duration.ofMinutes(10)); // only pub/sub can help
  }

  @AfterEach
  void tearDown() {
    if (reader != null) reader.close();
    if (redis != null) redis.close();
  }

  @Test
  void changesReachOtherServersThroughPubSub() throws Exception {
    Rule r = Rule.tokenBucket("api", 10, Duration.ofSeconds(1), 20);
    writer.put(r);
    waitFor(() -> reader.get("api").isPresent(), "new rule never arrived");
    assertEquals(r, reader.get("api").get());

    writer.put(Rule.tokenBucket("api", 99, Duration.ofSeconds(1), 99));
    waitFor(() -> reader.get("api").get().limit() == 99, "updated rule never arrived");

    assertTrue(writer.delete("api"));
    waitFor(() -> reader.get("api").isEmpty(), "deleted rule never left");
    assertFalse(writer.delete("api"));
  }

  @Test
  void reloadSkipsInvalidRulesAndKeepsTheRest() {
    writer.put(Rule.tokenBucket("good", 1, Duration.ofSeconds(1), 1));
    redis.call("HSET", prefix + ":rules", "bad", "{\"limit\":-1}");
    RuleStore fresh = new RuleStore(redis, prefix);
    fresh.reload();
    assertTrue(fresh.get("good").isPresent());
    assertTrue(fresh.get("bad").isEmpty());
  }

  private static void waitFor(BooleanSupplier condition, String message) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) return;
      Thread.sleep(10);
    }
    fail(message);
  }
}
