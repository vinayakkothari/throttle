package io.github.vinayakkothari.throttle.server;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Counters and a latency histogram in the Prometheus text format.
 *
 * <p>Labels are the rule and the outcome, never the key. Keys are tenant ids or IPs, and a
 * label per key would create a new time series for every caller and take Prometheus down.
 */
public final class Metrics {
  private static final double[] BUCKETS = {
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1
  };

  private final Map<String, LongAdder> decisions = new ConcurrentHashMap<>();
  private final Map<String, Histogram> latency = new ConcurrentHashMap<>();
  private final LongAdder redisErrors = new LongAdder();
  private final Map<String, LongSupplier> gauges = new ConcurrentHashMap<>();
  private final Map<String, LongSupplier> counters = new ConcurrentHashMap<>();

  public void decision(String rule, boolean allowed, boolean degraded) {
    String labels =
        "rule=\"" + escape(rule) + "\",result=\"" + (allowed ? "allowed" : "denied")
            + "\",degraded=\"" + degraded + "\"";
    decisions.computeIfAbsent(labels, k -> new LongAdder()).increment();
  }

  public void latency(String endpoint, long nanos) {
    latency.computeIfAbsent(endpoint, k -> new Histogram()).observe(nanos / 1e9);
  }

  public void redisError() {
    redisErrors.increment();
  }

  public void gauge(String name, LongSupplier value) {
    gauges.put(name, value);
  }

  /** A counter whose value lives elsewhere, read at scrape time. */
  public void counter(String name, LongSupplier value) {
    counters.put(name, value);
  }

  public String scrape() {
    StringBuilder sb = new StringBuilder(4096);
    sb.append("# HELP throttle_decisions_total Rate limit decisions by rule and result.\n");
    sb.append("# TYPE throttle_decisions_total counter\n");
    new TreeMap<>(decisions)
        .forEach((labels, n) -> line(sb, "throttle_decisions_total{" + labels + "}", n.sum()));

    sb.append("# HELP throttle_request_duration_seconds Time to answer an API call.\n");
    sb.append("# TYPE throttle_request_duration_seconds histogram\n");
    new TreeMap<>(latency).forEach((endpoint, h) -> h.write(sb, endpoint));

    sb.append("# HELP throttle_redis_errors_total Calls that failed because Redis was unavailable.\n");
    sb.append("# TYPE throttle_redis_errors_total counter\n");
    line(sb, "throttle_redis_errors_total", redisErrors.sum());

    new TreeMap<>(counters)
        .forEach(
            (name, value) -> {
              sb.append("# TYPE ").append(name).append(" counter\n");
              line(sb, name, value.getAsLong());
            });
    new TreeMap<>(gauges)
        .forEach(
            (name, value) -> {
              sb.append("# TYPE ").append(name).append(" gauge\n");
              line(sb, name, value.getAsLong());
            });
    return sb.toString();
  }

  private static void line(StringBuilder sb, String name, Object value) {
    sb.append(name).append(' ').append(value).append('\n');
  }

  private static String escape(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
  }

  private static final class Histogram {
    private final LongAdder[] counts = new LongAdder[BUCKETS.length];
    private final LongAdder count = new LongAdder();
    private final DoubleAdder sum = new DoubleAdder();

    Histogram() {
      for (int i = 0; i < counts.length; i++) counts[i] = new LongAdder();
    }

    void observe(double seconds) {
      for (int i = 0; i < BUCKETS.length; i++) {
        if (seconds <= BUCKETS[i]) {
          counts[i].increment();
          break;
        }
      }
      count.increment();
      sum.add(seconds);
    }

    void write(StringBuilder sb, String endpoint) {
      String base = "throttle_request_duration_seconds";
      String ep = "endpoint=\"" + escape(endpoint) + "\"";
      long cumulative = 0;
      for (int i = 0; i < BUCKETS.length; i++) {
        cumulative += counts[i].sum();
        line(sb, base + "_bucket{" + ep + ",le=\"" + BUCKETS[i] + "\"}", cumulative);
      }
      line(sb, base + "_bucket{" + ep + ",le=\"+Inf\"}", count.sum());
      line(sb, base + "_sum{" + ep + "}", sum.sum());
      line(sb, base + "_count{" + ep + "}", count.sum());
    }
  }
}
