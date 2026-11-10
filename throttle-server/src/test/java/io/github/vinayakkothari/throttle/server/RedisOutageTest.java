package io.github.vinayakkothari.throttle.server;

import static io.github.vinayakkothari.throttle.server.Api.call;
import static io.github.vinayakkothari.throttle.server.Api.json;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Cut Redis away from a running server and check each rule's fail mode, then recovery. */
class RedisOutageTest {
  private CuttableProxy proxy;
  private ThrottleServer server;

  @BeforeEach
  void start() throws Exception {
    Api.assumeRedis();
    proxy = new CuttableProxy(Api.REDIS_URL);
    server = Api.start(proxy.url(), null);
    call(server, "PUT", "/v1/rules/capacity", "{\"limit\":100,\"period\":\"1s\",\"failMode\":\"open\"}", null);
    call(server, "PUT", "/v1/rules/login", "{\"limit\":5,\"period\":\"1m\",\"failMode\":\"closed\"}", null);
  }

  @AfterEach
  void stop() throws Exception {
    if (server != null) server.close();
    if (proxy != null) proxy.close();
  }

  private Map<String, Object> check(String rule) throws Exception {
    return json(call(server, "POST", "/v1/check", "{\"rule\":\"" + rule + "\",\"key\":\"k\"}", null));
  }

  @Test
  void failModesDecideWhileRedisIsDownAndRecoveryIsAutomatic() throws Exception {
    assertNull(check("capacity").get("degraded"));

    proxy.cut();
    long start = System.nanoTime();
    Map<String, Object> open = check("capacity");
    Map<String, Object> closed = check("login");
    long tookMs = (System.nanoTime() - start) / 1_000_000;

    assertEquals(true, open.get("allowed"), "fail open lets traffic through");
    assertEquals(true, open.get("degraded"));
    assertEquals(false, closed.get("allowed"), "fail closed refuses traffic");
    assertEquals(true, closed.get("degraded"));
    assertTrue(tookMs < 2_000, "an outage must not stall callers, took " + tookMs + " ms");
    assertEquals(503, call(server, "GET", "/health", null, null).statusCode());
    assertTrue(call(server, "GET", "/metrics", null, null).body()
        .contains("throttle_decisions_total{rule=\"login\",result=\"denied\",degraded=\"true\"} 1"));

    proxy.restore();
    Map<String, Object> back = check("login");
    assertEquals(true, back.get("allowed"));
    assertNull(back.get("degraded"), "normal decisions resume once Redis is back");
    assertEquals(200, call(server, "GET", "/health", null, null).statusCode());
  }

  /**
   * Redis restarts between two requests. Every pooled connection is now dead, but nobody has
   * used them yet. The next decision must still be a real one, not a degraded guess.
   */
  @Test
  void deadPooledConnectionsAreReplacedWithoutADegradedAnswer() throws Exception {
    for (int i = 0; i < 5; i++) assertNull(check("capacity").get("degraded")); // fill the pool
    proxy.cut();
    proxy.restore();
    for (int i = 0; i < 5; i++) {
      assertNull(check("capacity").get("degraded"), "request " + i + " after the restart");
    }
  }
}
