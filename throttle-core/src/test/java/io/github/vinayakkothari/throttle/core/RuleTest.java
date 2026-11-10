package io.github.vinayakkothari.throttle.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vinayakkothari.throttle.core.redis.RedisConfig;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RuleTest {

  @Test
  void fillsDefaults() {
    Rule r = Rule.fromJson(Json.parseObject("{\"limit\":100,\"period\":\"1s\"}"), "api");
    assertEquals(Algorithm.TOKEN_BUCKET, r.algorithm());
    assertEquals(100, r.burst());
    assertEquals(FailMode.OPEN, r.failMode());
    assertEquals(Duration.ofSeconds(1), r.period());
  }

  @Test
  void roundTripsThroughJson() {
    Rule r =
        new Rule("login", Algorithm.SLIDING_WINDOW_LOG, 5, Duration.ofMinutes(1), 0,
            FailMode.CLOSED, Map.of("trusted-ip", 50L));
    Rule back = Rule.fromJson(Json.parseObject(Json.write(r.toJson())), null);
    assertEquals(r, back);
  }

  @Test
  void overridesScaleTheBurst() {
    Rule r = Rule.tokenBucket("api", 100, Duration.ofSeconds(1), 200).withOverrides(Map.of("vip", 1000L));
    assertEquals(100, r.limitFor("anyone"));
    assertEquals(200, r.capacityFor("anyone"));
    assertEquals(1000, r.limitFor("vip"));
    assertEquals(2000, r.capacityFor("vip"));
    assertEquals(1.0, r.ratePerMillisFor("vip"));
  }

  @Test
  void rejectsBadRules() {
    String[] bad = {
      "{\"limit\":0,\"period\":\"1s\"}",
      "{\"limit\":10}",
      "{\"limit\":10,\"period\":\"soon\"}",
      "{\"limit\":10,\"period\":\"1s\",\"algorithm\":\"leaky\"}",
      "{\"limit\":10,\"period\":\"1s\",\"failMode\":\"maybe\"}",
      "{\"limit\":1.5,\"period\":\"1s\"}",
      "{\"limit\":10,\"period\":\"1s\",\"overrides\":{\"a\":0}}",
      "{\"limit\":10,\"period\":\"1s\",\"overrides\":[1]}",
    };
    for (String json : bad) {
      assertThrows(IllegalArgumentException.class, () -> Rule.fromJson(Json.parseObject(json), "x"), json);
    }
    assertThrows(IllegalArgumentException.class, () -> Rule.of("has space", Algorithm.TOKEN_BUCKET, 1, Duration.ofSeconds(1)));
    assertThrows(IllegalArgumentException.class,
        () -> Rule.fromJson(Json.parseObject("{\"id\":\"a\",\"limit\":1,\"period\":\"1s\"}"), "b"));
  }

  @Test
  void parsesAndFormatsDurations() {
    assertEquals(Duration.ofMillis(250), Durations.parse("250ms"));
    assertEquals(Duration.ofMinutes(5), Durations.parse(" 5M "));
    assertEquals("1m", Durations.format(Duration.ofSeconds(60)));
    assertEquals("1500ms", Durations.format(Duration.ofMillis(1500)));
    assertEquals("2d", Durations.format(Duration.ofDays(2)));
    assertThrows(IllegalArgumentException.class, () -> Durations.parse("1 week"));
  }

  @Test
  void parsesRedisUrls() {
    RedisConfig c = RedisConfig.parse("rediss://app:p%40ss@cache.internal:6380/2");
    assertEquals("cache.internal", c.host());
    assertEquals(6380, c.port());
    assertEquals("app", c.username());
    assertEquals("p@ss", c.password());
    assertEquals(2, c.database());
    assertTrue(c.tls());
    assertFalse(c.describe().contains("p@ss"));

    RedisConfig plain = RedisConfig.parse("redis://secret@localhost");
    assertNull(plain.username());
    assertEquals("secret", plain.password());
    assertEquals(6379, plain.port());
    assertThrows(IllegalArgumentException.class, () -> RedisConfig.parse("http://localhost"));
  }
}
