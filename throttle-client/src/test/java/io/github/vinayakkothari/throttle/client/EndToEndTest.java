package io.github.vinayakkothari.throttle.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.vinayakkothari.throttle.core.Decision;
import io.github.vinayakkothari.throttle.core.Rule;
import io.github.vinayakkothari.throttle.core.RuleStore;
import io.github.vinayakkothari.throttle.core.redis.RedisClient;
import io.github.vinayakkothari.throttle.server.ServerConfig;
import io.github.vinayakkothari.throttle.server.ThrottleServer;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The client against two real servers sharing one Redis. */
class EndToEndTest {
  private static final String REDIS = System.getenv().getOrDefault("REDIS_URL", "redis://localhost:6379");
  private ThrottleServer a;
  private ThrottleServer b;
  private String prefix;

  @BeforeEach
  void start() throws Exception {
    boolean up;
    try (RedisClient c = RedisClient.create(REDIS)) {
      up = c.ping();
    } catch (RuntimeException e) {
      up = false;
    }
    assumeTrue(up, "no Redis at " + REDIS + ", skipping");
    prefix = "test-" + UUID.randomUUID().toString().substring(0, 8);
    ServerConfig cfg = ServerConfig.defaults().withPort(0).withRedisUrl(REDIS).withPrefix(prefix);
    a = new ThrottleServer(cfg).start();
    b = new ThrottleServer(cfg).start();
    a.rules().put(Rule.tokenBucket("api", 100, Duration.ofHours(1), 100));
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (b.rules().get("api").isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(10); // server b hears about the rule through pub/sub
    }
    assertTrue(b.rules().get("api").isPresent(), "server b never received the rule");
  }

  @AfterEach
  void stop() {
    if (a != null) a.close();
    if (b != null) b.close();
  }

  private String url(ThrottleServer s) {
    return "http://127.0.0.1:" + s.port();
  }

  @Test
  void twoServersShareOneLimit() {
    try (ThrottleClient c =
        ThrottleClient.builder(new HttpBackend(List.of(url(a), url(b)), Duration.ofSeconds(1))).build()) {
      int allowed = 0;
      for (int i = 0; i < 150; i++) if (c.tryAcquire("api", "t").allowed()) allowed++;
      assertEquals(100, allowed, "requests alternate between servers but share one bucket");
    }
  }

  @Test
  void leasingAgainstARealServer() {
    try (ThrottleClient c =
        ThrottleClient.builder(HttpBackend.of(url(a))).leaseSize(20).build()) {
      int allowed = 0;
      for (int i = 0; i < 150; i++) if (c.tryAcquire("api", "t").allowed()) allowed++;
      assertEquals(100, allowed);
      assertTrue(c.stats().backendCalls() <= 7, "calls " + c.stats().backendCalls());
    }
  }

  @Test
  void failsOverToTheNextServer() throws Exception {
    int dead;
    try (ServerSocket s = new ServerSocket(0)) {
      dead = s.getLocalPort(); // nothing listens here once closed
    }
    HttpBackend backend =
        new HttpBackend(List.of("http://127.0.0.1:" + dead, url(a)), Duration.ofMillis(500));
    for (int i = 0; i < 4; i++) {
      Decision d = backend.acquire("api", "f", 1, false);
      assertTrue(d.allowed() && !d.degraded());
    }
    HttpBackend allDead = new HttpBackend(List.of("http://127.0.0.1:" + dead), Duration.ofMillis(500));
    assertThrows(BackendException.class, () -> allDead.acquire("api", "f", 1, false));
    backend.close();
    allDead.close();
  }

  @Test
  void libraryModeTalksToRedisDirectly() {
    RedisClient redis = RedisClient.create(REDIS);
    RedisBackend backend = new RedisBackend(redis, prefix);
    try (ThrottleClient c = ThrottleClient.builder(backend).build()) {
      RuleStore store = backend.ruleStore();
      assertTrue(store.get("api").isPresent());
      int allowed = 0;
      for (int i = 0; i < 120; i++) if (c.tryAcquire("api", "lib").allowed()) allowed++;
      assertEquals(100, allowed);
    }
  }
}
