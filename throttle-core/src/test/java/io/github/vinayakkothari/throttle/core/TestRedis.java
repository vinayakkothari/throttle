package io.github.vinayakkothari.throttle.core;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.vinayakkothari.throttle.core.redis.RedisClient;
import io.github.vinayakkothari.throttle.core.redis.RedisConfig;
import java.time.Duration;
import java.util.UUID;

/**
 * Tests that need Redis use the one at REDIS_URL (redis://localhost:6379 by default). Each test
 * gets its own key prefix, so tests never see each other's state. Without a Redis the tests are
 * skipped locally; CI always provides one.
 */
public final class TestRedis {
  public static final String URL = System.getenv().getOrDefault("REDIS_URL", "redis://localhost:6379");

  private TestRedis() {}

  public static RedisClient connect() {
    RedisClient client = new RedisClient(RedisConfig.parse(URL, Duration.ofSeconds(2), 64));
    boolean up;
    try {
      up = client.ping();
    } catch (RuntimeException e) {
      up = false;
    }
    if (!up) client.close();
    assumeTrue(up, "no Redis at " + URL + ", skipping");
    return client;
  }

  public static String prefix() {
    return "test-" + UUID.randomUUID().toString().substring(0, 8);
  }
}
