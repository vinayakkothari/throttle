package io.github.vinayakkothari.throttle.core;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The answer to "may this go ahead?".
 *
 * @param granted how many units were granted; for a plain check either the cost or zero
 * @param remaining units left for this key right now
 * @param limit the limit that applied to this key
 * @param retryAfterMillis when to try again if denied; -1 if the request can never fit
 * @param degraded true when Redis was unreachable and the rule's fail mode decided
 */
public record Decision(
    boolean allowed,
    long granted,
    long remaining,
    long limit,
    long retryAfterMillis,
    boolean degraded) {

  public static Decision allow(long granted, long remaining, long limit) {
    return new Decision(true, granted, remaining, limit, 0, false);
  }

  public static Decision deny(long remaining, long limit, long retryAfterMillis) {
    return new Decision(false, 0, remaining, limit, retryAfterMillis, false);
  }

  /** Redis was unavailable and the fail mode answered instead. */
  public static Decision degraded(boolean allow, long want, long limit) {
    return new Decision(allow, allow ? want : 0, 0, limit, allow ? 0 : 1000, true);
  }

  public Map<String, Object> toJson() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("allowed", allowed);
    m.put("granted", granted);
    m.put("remaining", remaining);
    m.put("limit", limit);
    m.put("retryAfterMs", retryAfterMillis);
    if (degraded) m.put("degraded", true);
    return m;
  }

  public static Decision fromJson(Map<String, Object> m) {
    return new Decision(
        Boolean.TRUE.equals(m.get("allowed")),
        longOf(m.get("granted")),
        longOf(m.get("remaining")),
        longOf(m.get("limit")),
        longOf(m.get("retryAfterMs")),
        Boolean.TRUE.equals(m.get("degraded")));
  }

  private static long longOf(Object o) {
    return o instanceof Number n ? n.longValue() : 0;
  }
}
