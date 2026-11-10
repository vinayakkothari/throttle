package io.github.vinayakkothari.throttle.core;

/** How a rule counts requests. */
public enum Algorithm {
  /** Allows bursts up to a capacity and refills at a steady rate. The default. */
  TOKEN_BUCKET("token_bucket", "tb"),
  /** Exact count over the last window. Memory grows with the limit. */
  SLIDING_WINDOW_LOG("sliding_window_log", "swl"),
  /** Close estimate over the last window in constant memory. */
  SLIDING_WINDOW_COUNTER("sliding_window_counter", "swc");

  private final String json;
  private final String shortName;

  Algorithm(String json, String shortName) {
    this.json = json;
    this.shortName = shortName;
  }

  public String json() {
    return json;
  }

  /** Part of the Redis key, so switching a rule's algorithm never reads the old structure. */
  public String shortName() {
    return shortName;
  }

  public static Algorithm fromJson(String s) {
    for (Algorithm a : values()) if (a.json.equalsIgnoreCase(s)) return a;
    throw new IllegalArgumentException(
        "unknown algorithm '" + s + "', use token_bucket, sliding_window_log or sliding_window_counter");
  }
}
