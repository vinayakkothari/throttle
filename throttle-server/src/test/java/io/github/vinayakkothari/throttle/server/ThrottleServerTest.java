package io.github.vinayakkothari.throttle.server;

import static io.github.vinayakkothari.throttle.server.Api.call;
import static io.github.vinayakkothari.throttle.server.Api.json;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ThrottleServerTest {
  private static final String TOKEN = "s3cret";
  private ThrottleServer server;

  @BeforeEach
  void start() throws Exception {
    Api.assumeRedis();
    server = Api.start(Api.REDIS_URL, TOKEN);
    var res = call(server, "PUT", "/v1/rules/api",
        "{\"limit\":3,\"period\":\"1m\",\"overrides\":{\"vip\":10}}", TOKEN);
    assertEquals(200, res.statusCode(), res.body());
  }

  @AfterEach
  void stop() {
    if (server != null) server.close();
  }

  @Test
  void checkAllowsThenDeniesWithHeaders() throws Exception {
    for (int i = 0; i < 3; i++) {
      var res = call(server, "POST", "/v1/check", "{\"rule\":\"api\",\"key\":\"t1\"}", null);
      assertEquals(200, res.statusCode());
      assertEquals(true, json(res).get("allowed"));
      assertEquals(String.valueOf(2 - i), res.headers().firstValue("RateLimit-Remaining").get());
    }
    var denied = call(server, "POST", "/v1/check", "{\"rule\":\"api\",\"key\":\"t1\"}", null);
    assertEquals(200, denied.statusCode(), "a denial is a successful answer");
    assertEquals(false, json(denied).get("allowed"));
    assertTrue(denied.headers().firstValue("Retry-After").isPresent());
  }

  @Test
  void acquireGrantsUpToWhatIsLeft() throws Exception {
    var res = call(server, "POST", "/v1/acquire", "{\"rule\":\"api\",\"key\":\"vip\",\"want\":50}", null);
    assertEquals(10L, json(res).get("granted"));
    assertEquals(10L, json(res).get("limit"));
  }

  @Test
  void rejectsBadRequestsClearly() throws Exception {
    assertEquals(404, call(server, "POST", "/v1/check", "{\"rule\":\"nope\",\"key\":\"k\"}", null).statusCode());
    assertEquals(400, call(server, "POST", "/v1/check", "{\"rule\":\"api\"}", null).statusCode());
    assertEquals(400, call(server, "POST", "/v1/check", "not json", null).statusCode());
    assertEquals(400, call(server, "POST", "/v1/check", "", null).statusCode());
    assertEquals(400, call(server, "POST", "/v1/check", "{\"rule\":\"api\",\"key\":\"k\",\"cost\":\"1\"}", null).statusCode());
    assertEquals(404, call(server, "GET", "/v2/anything", null, null).statusCode());
    var tooBig = "{\"rule\":\"api\",\"key\":\"" + "x".repeat(70_000) + "\"}";
    assertEquals(400, call(server, "POST", "/v1/check", tooBig, null).statusCode());
  }

  @Test
  void rulesNeedTheAdminTokenToChange() throws Exception {
    String body = "{\"limit\":1,\"period\":\"1s\"}";
    assertEquals(401, call(server, "PUT", "/v1/rules/x", body, null).statusCode());
    assertEquals(401, call(server, "PUT", "/v1/rules/x", body, "wrong").statusCode());
    assertEquals(200, call(server, "PUT", "/v1/rules/x", body, TOKEN).statusCode());
    assertEquals(400, call(server, "PUT", "/v1/rules/y", "{\"limit\":-1,\"period\":\"1s\"}", TOKEN).statusCode());

    var list = json(call(server, "GET", "/v1/rules", null, null));
    List<?> rules = (List<?>) list.get("rules");
    assertEquals(2, rules.size());
    assertEquals("api", ((Map<?, ?>) rules.get(0)).get("id"));

    assertEquals(401, call(server, "DELETE", "/v1/rules/x", null, null).statusCode());
    assertEquals(200, call(server, "DELETE", "/v1/rules/x", null, TOKEN).statusCode());
    assertEquals(404, call(server, "GET", "/v1/rules/x", null, null).statusCode());
  }

  @Test
  void exposesHealthAndMetrics() throws Exception {
    call(server, "POST", "/v1/check", "{\"rule\":\"api\",\"key\":\"m\"}", null);
    assertEquals(200, call(server, "GET", "/health", null, null).statusCode());
    String metrics = call(server, "GET", "/metrics", null, null).body();
    assertTrue(metrics.contains(
        "throttle_decisions_total{rule=\"api\",result=\"allowed\",degraded=\"false\"} 1"), metrics);
    assertTrue(metrics.contains("throttle_request_duration_seconds_count{endpoint=\"check\"} 1"));
    assertTrue(metrics.contains("throttle_rules 1"));
  }
}
