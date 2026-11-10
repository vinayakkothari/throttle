package io.github.vinayakkothari.throttle.core.redis;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Where Redis lives and how to talk to it.
 *
 * <p>Parsed from a URL in the usual form: {@code redis://[user:password@]host[:port][/db]}, or
 * {@code rediss://} for TLS.
 */
public record RedisConfig(
    String host,
    int port,
    String username,
    String password,
    int database,
    boolean tls,
    Duration timeout,
    int poolSize) {

  public static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(500);
  public static final int DEFAULT_POOL = 32;

  public RedisConfig {
    if (host == null || host.isBlank()) throw new IllegalArgumentException("redis host is empty");
    if (port < 1 || port > 65535) throw new IllegalArgumentException("bad redis port " + port);
    if (database < 0) throw new IllegalArgumentException("bad redis database " + database);
    if (timeout == null || timeout.isNegative() || timeout.isZero()) timeout = DEFAULT_TIMEOUT;
    if (poolSize < 1) throw new IllegalArgumentException("pool size must be at least 1");
  }

  public static RedisConfig parse(String url) {
    return parse(url, DEFAULT_TIMEOUT, DEFAULT_POOL);
  }

  public static RedisConfig parse(String url, Duration timeout, int poolSize) {
    URI uri = URI.create(url.trim());
    String scheme = uri.getScheme();
    if (!"redis".equals(scheme) && !"rediss".equals(scheme)) {
      throw new IllegalArgumentException("redis url must start with redis:// or rediss://");
    }
    String user = null;
    String pass = null;
    String info = uri.getRawUserInfo();
    if (info != null && !info.isEmpty()) {
      int colon = info.indexOf(':');
      if (colon < 0) {
        pass = decode(info); // redis://password@host is a common shorthand
      } else {
        user = colon == 0 ? null : decode(info.substring(0, colon));
        pass = decode(info.substring(colon + 1));
      }
    }
    int db = 0;
    String path = uri.getPath();
    if (path != null && path.length() > 1) db = Integer.parseInt(path.substring(1));
    int port = uri.getPort() == -1 ? 6379 : uri.getPort();
    return new RedisConfig(
        uri.getHost(), port, user, pass, db, "rediss".equals(scheme), timeout, poolSize);
  }

  private static String decode(String s) {
    return URLDecoder.decode(s, StandardCharsets.UTF_8);
  }

  /** The address without credentials, safe to log. */
  public String describe() {
    return (tls ? "rediss://" : "redis://") + host + ":" + port + "/" + database;
  }
}
