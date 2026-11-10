package io.github.vinayakkothari.throttle.core;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A named limit, for example "100 requests per second per tenant, bursts up to 200".
 *
 * <p>The caller decides what the key is (a tenant id, an API key, an IP, a user and route pair)
 * and names the rule to apply. Overrides give particular keys their own limit, which is how a
 * paying tenant gets more than the free tier.
 *
 * @param limit requests allowed per period
 * @param burst token bucket capacity; ignored by the sliding window algorithms
 * @param overrides per key limits; for a token bucket the burst scales by the same ratio
 */
public record Rule(
    String id,
    Algorithm algorithm,
    long limit,
    Duration period,
    long burst,
    FailMode failMode,
    Map<String, Long> overrides) {

  private static final Pattern ID = Pattern.compile("[A-Za-z0-9_.-]{1,64}");
  public static final int MAX_KEY_LENGTH = 256;

  public Rule {
    if (id == null || !ID.matcher(id).matches()) {
      throw new IllegalArgumentException("rule id must be 1 to 64 letters, digits, _ . or -");
    }
    if (algorithm == null) algorithm = Algorithm.TOKEN_BUCKET;
    if (limit < 1) throw new IllegalArgumentException("limit must be at least 1");
    if (period == null || period.toMillis() < 1) {
      throw new IllegalArgumentException("period must be at least 1ms");
    }
    if (burst <= 0) burst = limit;
    if (failMode == null) failMode = FailMode.OPEN;
    overrides = overrides == null ? Map.of() : Map.copyOf(overrides);
    for (Map.Entry<String, Long> e : overrides.entrySet()) {
      if (e.getValue() == null || e.getValue() < 1) {
        throw new IllegalArgumentException("override for '" + e.getKey() + "' must be at least 1");
      }
    }
  }

  public static Rule tokenBucket(String id, long limit, Duration period, long burst) {
    return new Rule(id, Algorithm.TOKEN_BUCKET, limit, period, burst, FailMode.OPEN, Map.of());
  }

  public static Rule of(String id, Algorithm algorithm, long limit, Duration period) {
    return new Rule(id, algorithm, limit, period, 0, FailMode.OPEN, Map.of());
  }

  public Rule withFailMode(FailMode mode) {
    return new Rule(id, algorithm, limit, period, burst, mode, overrides);
  }

  public Rule withOverrides(Map<String, Long> o) {
    return new Rule(id, algorithm, limit, period, burst, failMode, o);
  }

  /** The limit for this key, after overrides. */
  public long limitFor(String key) {
    return overrides.getOrDefault(key, limit);
  }

  /** Token bucket capacity for this key. An override keeps the rule's burst to limit ratio. */
  public long capacityFor(String key) {
    Long o = overrides.get(key);
    if (o == null) return burst;
    return Math.max(1, Math.round((double) burst * o / limit));
  }

  /** Requests per millisecond for this key. */
  public double ratePerMillisFor(String key) {
    return (double) limitFor(key) / period.toMillis();
  }

  public Map<String, Object> toJson() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("id", id);
    m.put("algorithm", algorithm.json());
    m.put("limit", limit);
    m.put("period", Durations.format(period));
    if (algorithm == Algorithm.TOKEN_BUCKET) m.put("burst", burst);
    m.put("failMode", failMode.json());
    if (!overrides.isEmpty()) m.put("overrides", new LinkedHashMap<>(overrides));
    return m;
  }

  /** Build a rule from JSON. {@code idFromPath}, when given, wins over any id in the body. */
  public static Rule fromJson(Map<String, Object> m, String idFromPath) {
    Object rawId = m.get("id");
    String id = idFromPath != null ? idFromPath : rawId == null ? null : rawId.toString();
    if (idFromPath != null && rawId != null && !idFromPath.equals(rawId.toString())) {
      throw new IllegalArgumentException("id in the body does not match the id in the path");
    }
    Algorithm algorithm =
        m.containsKey("algorithm") ? Algorithm.fromJson(str(m, "algorithm")) : null;
    long limit = num(m, "limit", -1);
    Object rawPeriod = m.get("period");
    if (rawPeriod == null) throw new IllegalArgumentException("period is required, like \"1s\"");
    Duration period =
        rawPeriod instanceof Number n
            ? Duration.ofMillis(n.longValue())
            : Durations.parse(rawPeriod.toString());
    long burst = num(m, "burst", 0);
    FailMode fail = m.containsKey("failMode") ? FailMode.fromJson(str(m, "failMode")) : null;
    Map<String, Long> overrides = new LinkedHashMap<>();
    Object raw = m.get("overrides");
    if (raw instanceof Map<?, ?> om) {
      for (Map.Entry<?, ?> e : om.entrySet()) {
        if (!(e.getValue() instanceof Number n)) {
          throw new IllegalArgumentException("override values must be numbers");
        }
        overrides.put(String.valueOf(e.getKey()), n.longValue());
      }
    } else if (raw != null) {
      throw new IllegalArgumentException("overrides must be an object of key to limit");
    }
    return new Rule(id, algorithm, limit, period, burst, fail, overrides);
  }

  private static String str(Map<String, Object> m, String k) {
    Object v = m.get(k);
    if (!(v instanceof String s)) throw new IllegalArgumentException(k + " must be a string");
    return s;
  }

  private static long num(Map<String, Object> m, String k, long fallback) {
    Object v = m.get(k);
    if (v == null) return fallback;
    if (!(v instanceof Number n) || n.doubleValue() != n.longValue()) {
      throw new IllegalArgumentException(k + " must be a whole number");
    }
    return n.longValue();
  }
}
