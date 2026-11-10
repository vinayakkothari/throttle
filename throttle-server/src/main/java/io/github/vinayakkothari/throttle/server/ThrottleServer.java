package io.github.vinayakkothari.throttle.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.vinayakkothari.throttle.core.Decision;
import io.github.vinayakkothari.throttle.core.FailMode;
import io.github.vinayakkothari.throttle.core.Json;
import io.github.vinayakkothari.throttle.core.Limiter;
import io.github.vinayakkothari.throttle.core.Rule;
import io.github.vinayakkothari.throttle.core.RuleStore;
import io.github.vinayakkothari.throttle.core.redis.RedisClient;
import io.github.vinayakkothari.throttle.core.redis.RedisConfig;
import io.github.vinayakkothari.throttle.core.redis.RedisException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The rate limiting service: a JDK HTTP server where every request runs on its own virtual
 * thread.
 *
 * <pre>
 *   POST   /v1/check         {"rule","key","cost"}  all or nothing
 *   POST   /v1/acquire       {"rule","key","want"}  up to want, for client-side leasing
 *   GET    /v1/rules         list rules
 *   GET    /v1/rules/{id}    one rule
 *   PUT    /v1/rules/{id}    create or replace (admin token if configured)
 *   DELETE /v1/rules/{id}    remove (admin token if configured)
 *   GET    /health           200 when Redis answers, 503 otherwise
 *   GET    /metrics          Prometheus text format
 * </pre>
 *
 * A denied request still gets HTTP 200. The call to the limiter worked; the answer is no. HTTP
 * 429 is what the calling app sends to its own user.
 */
public final class ThrottleServer implements AutoCloseable {
  private static final Logger LOG = System.getLogger(ThrottleServer.class.getName());
  private static final int MAX_BODY = 64 * 1024;

  static {
    // The JDK server leaves Nagle's algorithm on. With small JSON replies and keep-alive
    // connections, Nagle and delayed ACKs together held every response for tens of
    // milliseconds: 44 ms per call on localhost, against 1.6 ms with this set.
    if (System.getProperty("sun.net.httpserver.nodelay") == null) {
      System.setProperty("sun.net.httpserver.nodelay", "true");
    }
  }

  private final ServerConfig config;
  private final RedisClient redis;
  private final RuleStore rules;
  private final Limiter limiter;
  private final Metrics metrics = new Metrics();
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private HttpServer http;

  public ThrottleServer(ServerConfig config) {
    this.config = config;
    this.redis =
        new RedisClient(
            RedisConfig.parse(config.redisUrl(), config.redisTimeout(), config.redisPool()));
    this.rules = new RuleStore(redis, config.prefix());
    this.limiter = new Limiter(redis, config.prefix());
  }

  public ThrottleServer start() throws IOException {
    try {
      rules.start(config.ruleRefresh());
      if (config.rulesFile() != null) loadRulesFile(Path.of(config.rulesFile()));
    } catch (RedisException e) {
      // Start anyway: fail-open rules keep the callers working, and the timer retries.
      LOG.log(Level.WARNING, "redis unavailable at startup: {0}", e.getMessage());
    }
    metrics.gauge("throttle_rules", () -> rules.all().size());
    metrics.counter("throttle_redis_calls_total", redis::calls);

    http = HttpServer.create(new InetSocketAddress(config.host(), config.port()), 1024);
    http.setExecutor(executor);
    http.createContext("/", this::handle);
    http.start();
    LOG.log(Level.INFO, "throttle listening on port {0}, redis {1}",
        port(), redis.config().describe());
    return this;
  }

  public int port() {
    return http.getAddress().getPort();
  }

  public RuleStore rules() {
    return rules;
  }

  public Metrics metrics() {
    return metrics;
  }

  // ------------------------------------------------------------------------- routing

  private void handle(HttpExchange ex) throws IOException {
    long start = System.nanoTime();
    String path = ex.getRequestURI().getPath();
    String method = ex.getRequestMethod();
    String endpoint = "other";
    try {
      if (path.equals("/v1/check") && method.equals("POST")) {
        endpoint = "check";
        decide(ex, false);
      } else if (path.equals("/v1/acquire") && method.equals("POST")) {
        endpoint = "acquire";
        decide(ex, true);
      } else if (path.equals("/v1/rules") && method.equals("GET")) {
        endpoint = "rules";
        List<Object> all = new ArrayList<>();
        rules.all().stream()
            .sorted(Comparator.comparing(Rule::id))
            .forEach(r -> all.add(r.toJson()));
        send(ex, 200, Map.of("rules", all));
      } else if (path.startsWith("/v1/rules/")) {
        endpoint = "rules";
        rule(ex, method, path.substring("/v1/rules/".length()));
      } else if (path.equals("/health") && method.equals("GET")) {
        endpoint = "health";
        boolean ok = redis.ping();
        send(ex, ok ? 200 : 503, Map.of("status", ok ? "ok" : "redis unavailable"));
      } else if (path.equals("/metrics") && method.equals("GET")) {
        endpoint = "metrics";
        sendText(ex, 200, metrics.scrape());
      } else {
        send(ex, 404, error("no such endpoint " + method + " " + path));
      }
    } catch (BadRequest | IllegalArgumentException e) {
      send(ex, 400, error(e.getMessage()));
    } catch (RedisException e) {
      metrics.redisError();
      send(ex, 503, error("redis unavailable"));
    } catch (RuntimeException e) {
      LOG.log(Level.ERROR, "unexpected error on " + path, e);
      send(ex, 500, error("internal error"));
    } finally {
      metrics.latency(endpoint, System.nanoTime() - start);
      ex.close();
    }
  }

  // ----------------------------------------------------------------------- decisions

  private void decide(HttpExchange ex, boolean partial) throws IOException {
    Map<String, Object> body = readJson(ex);
    String ruleId = requireString(body, "rule");
    String key = requireString(body, "key");
    long want = optionalLong(body, partial ? "want" : "cost", 1);

    Rule rule = rules.get(ruleId).orElse(null);
    if (rule == null) {
      send(ex, 404, error("no rule named '" + ruleId + "'"));
      return;
    }
    Decision d;
    try {
      d = limiter.acquire(rule, key, want, partial);
    } catch (RedisException e) {
      metrics.redisError();
      d = Decision.degraded(rule.failMode() == FailMode.OPEN, want, rule.limitFor(key));
    }
    metrics.decision(rule.id(), d.allowed(), d.degraded());

    var h = ex.getResponseHeaders();
    h.set("RateLimit-Limit", Long.toString(d.limit()));
    h.set("RateLimit-Remaining", Long.toString(d.remaining()));
    if (!d.allowed() && d.retryAfterMillis() > 0) {
      h.set("Retry-After", Long.toString((d.retryAfterMillis() + 999) / 1000));
    }
    send(ex, 200, d.toJson());
  }

  // --------------------------------------------------------------------------- rules

  private void rule(HttpExchange ex, String method, String id) throws IOException {
    switch (method) {
      case "GET" -> {
        var rule = rules.get(id);
        if (rule.isPresent()) send(ex, 200, rule.get().toJson());
        else send(ex, 404, error("no rule named '" + id + "'"));
      }
      case "PUT" -> {
        if (!authorised(ex)) return;
        Rule rule = Rule.fromJson(readJson(ex), id);
        rules.put(rule);
        send(ex, 200, rule.toJson());
      }
      case "DELETE" -> {
        if (!authorised(ex)) return;
        if (rules.delete(id)) send(ex, 200, Map.of("deleted", id));
        else send(ex, 404, error("no rule named '" + id + "'"));
      }
      default -> send(ex, 405, error("use GET, PUT or DELETE"));
    }
  }

  private boolean authorised(HttpExchange ex) throws IOException {
    String token = config.adminToken();
    if (token == null) return true;
    String header = ex.getRequestHeaders().getFirst("Authorization");
    String given = header != null && header.startsWith("Bearer ") ? header.substring(7) : "";
    // constant time, so the token cannot be guessed one character at a time
    boolean ok =
        MessageDigest.isEqual(
            given.getBytes(StandardCharsets.UTF_8), token.getBytes(StandardCharsets.UTF_8));
    if (!ok) send(ex, 401, error("admin token required to change rules"));
    return ok;
  }

  private void loadRulesFile(Path file) throws IOException {
    Object parsed = Json.parse(Files.readString(file));
    if (!(parsed instanceof List<?> list)) {
      throw new IllegalArgumentException(file + " must hold a JSON array of rules");
    }
    for (Object item : list) {
      @SuppressWarnings("unchecked")
      Map<String, Object> m = (Map<String, Object>) item;
      rules.put(Rule.fromJson(m, null));
    }
    LOG.log(Level.INFO, "loaded {0} rules from {1}", list.size(), file);
  }

  // ------------------------------------------------------------------------- plumbing

  private static Map<String, Object> readJson(HttpExchange ex) throws IOException {
    try (InputStream in = ex.getRequestBody()) {
      byte[] bytes = in.readNBytes(MAX_BODY + 1);
      if (bytes.length > MAX_BODY) throw new BadRequest("body larger than 64 KB");
      if (bytes.length == 0) throw new BadRequest("a JSON body is required");
      return Json.parseObject(new String(bytes, StandardCharsets.UTF_8));
    }
  }

  private static String requireString(Map<String, Object> body, String field) {
    Object v = body.get(field);
    if (!(v instanceof String s) || s.isEmpty()) throw new BadRequest(field + " is required");
    return s;
  }

  private static long optionalLong(Map<String, Object> body, String field, long fallback) {
    Object v = body.get(field);
    if (v == null) return fallback;
    if (!(v instanceof Long n)) throw new BadRequest(field + " must be a whole number");
    return n;
  }

  private static Map<String, Object> error(String message) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("error", message);
    return m;
  }

  private static void send(HttpExchange ex, int status, Map<String, Object> body)
      throws IOException {
    byte[] bytes = Json.write(body).getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", "application/json");
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = ex.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static void sendText(HttpExchange ex, int status, String text) throws IOException {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4");
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = ex.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static final class BadRequest extends RuntimeException {
    private static final long serialVersionUID = 1L;

    BadRequest(String message) {
      super(message);
    }
  }

  @Override
  public void close() {
    if (http != null) http.stop(0);
    executor.shutdownNow();
    rules.close();
    redis.close();
  }
}
