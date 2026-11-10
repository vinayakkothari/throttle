package io.github.vinayakkothari.throttle.server;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.vinayakkothari.throttle.core.Json;
import io.github.vinayakkothari.throttle.core.redis.RedisClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/** Test helpers: a running server on a random port and a tiny HTTP client for it. */
final class Api {
  static final String REDIS_URL =
      System.getenv().getOrDefault("REDIS_URL", "redis://localhost:6379");
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private Api() {}

  static void assumeRedis() {
    boolean up;
    try (RedisClient c = RedisClient.create(REDIS_URL)) {
      up = c.ping();
    } catch (RuntimeException e) {
      up = false;
    }
    assumeTrue(up, "no Redis at " + REDIS_URL + ", skipping");
  }

  static ThrottleServer start(String redisUrl, String adminToken) throws Exception {
    ServerConfig cfg =
        ServerConfig.defaults()
            .withPort(0)
            .withRedisUrl(redisUrl)
            .withPrefix("test-" + UUID.randomUUID().toString().substring(0, 8))
            .withAdminToken(adminToken);
    return new ThrottleServer(cfg).start();
  }

  static HttpResponse<String> call(ThrottleServer s, String method, String path, String body,
      String token) throws Exception {
    HttpRequest.Builder b =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + s.port() + path))
            .timeout(Duration.ofSeconds(5));
    if (token != null) b.header("Authorization", "Bearer " + token);
    b.method(method, body == null
        ? HttpRequest.BodyPublishers.noBody()
        : HttpRequest.BodyPublishers.ofString(body));
    return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  static Map<String, Object> json(HttpResponse<String> res) {
    return Json.parseObject(res.body());
  }
}
