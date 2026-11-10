package io.github.vinayakkothari.throttle.server;

import io.github.vinayakkothari.throttle.core.Durations;
import java.time.Duration;
import java.util.Map;

/**
 * Settings, read from environment variables so the same image runs anywhere.
 *
 * <pre>
 *   THROTTLE_PORT               8080
 *   THROTTLE_REDIS_URL          redis://localhost:6379
 *   THROTTLE_REDIS_TIMEOUT      200ms    give up on Redis after this long
 *   THROTTLE_REDIS_POOL         64       connections per server
 *   THROTTLE_PREFIX             throttle every Redis key starts with this
 *   THROTTLE_ADMIN_TOKEN                 if set, required to change rules
 *   THROTTLE_RULES_FILE                  JSON array of rules to load at startup
 *   THROTTLE_RULE_REFRESH       30s      full reload interval, a safety net for pub/sub
 * </pre>
 */
public record ServerConfig(
    String host,
    int port,
    String redisUrl,
    Duration redisTimeout,
    int redisPool,
    String prefix,
    String adminToken,
    String rulesFile,
    Duration ruleRefresh) {

  public static ServerConfig defaults() {
    return fromEnv(Map.of());
  }

  public static ServerConfig fromEnv(Map<String, String> env) {
    return new ServerConfig(
        env.getOrDefault("THROTTLE_HOST", "0.0.0.0"),
        Integer.parseInt(env.getOrDefault("THROTTLE_PORT", "8080")),
        env.getOrDefault("THROTTLE_REDIS_URL", "redis://localhost:6379"),
        Durations.parse(env.getOrDefault("THROTTLE_REDIS_TIMEOUT", "200ms")),
        Integer.parseInt(env.getOrDefault("THROTTLE_REDIS_POOL", "64")),
        env.getOrDefault("THROTTLE_PREFIX", "throttle"),
        blankToNull(env.get("THROTTLE_ADMIN_TOKEN")),
        blankToNull(env.get("THROTTLE_RULES_FILE")),
        Durations.parse(env.getOrDefault("THROTTLE_RULE_REFRESH", "30s")));
  }

  public ServerConfig withPort(int p) {
    return new ServerConfig(
        host, p, redisUrl, redisTimeout, redisPool, prefix, adminToken, rulesFile, ruleRefresh);
  }

  public ServerConfig withRedisUrl(String url) {
    return new ServerConfig(
        host, port, url, redisTimeout, redisPool, prefix, adminToken, rulesFile, ruleRefresh);
  }

  public ServerConfig withPrefix(String p) {
    return new ServerConfig(
        host, port, redisUrl, redisTimeout, redisPool, p, adminToken, rulesFile, ruleRefresh);
  }

  public ServerConfig withAdminToken(String t) {
    return new ServerConfig(
        host, port, redisUrl, redisTimeout, redisPool, prefix, t, rulesFile, ruleRefresh);
  }

  private static String blankToNull(String s) {
    return s == null || s.isBlank() ? null : s;
  }
}
