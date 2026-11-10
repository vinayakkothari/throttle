package io.github.vinayakkothari.throttle.bench;

/**
 * A latency histogram: 1 microsecond buckets up to 10 ms, then 100 microsecond buckets up to 2 s.
 * One per worker, merged at the end, so recording never contends.
 */
final class Latency {
  private static final int FINE = 10_000; // micros covered at 1 us resolution
  private static final int COARSE = 19_900; // 100 us buckets from 10 ms to 2 s
  private final long[] counts = new long[FINE + COARSE + 1];
  private long total;

  void record(long nanos) {
    long micros = nanos / 1_000;
    int bucket =
        micros < FINE
            ? (int) micros
            : (int) Math.min(FINE + COARSE, FINE + (micros - FINE) / 100);
    counts[bucket]++;
    total++;
  }

  void merge(Latency other) {
    for (int i = 0; i < counts.length; i++) counts[i] += other.counts[i];
    total += other.total;
  }

  long count() {
    return total;
  }

  /** Percentile in milliseconds, for example {@code percentile(99)}. */
  double percentile(double p) {
    if (total == 0) return 0;
    long rank = (long) Math.ceil(p / 100.0 * total);
    long seen = 0;
    for (int i = 0; i < counts.length; i++) {
      seen += counts[i];
      if (seen >= rank) return (i < FINE ? i : FINE + (i - FINE) * 100L) / 1000.0;
    }
    return (FINE + COARSE * 100L) / 1000.0;
  }
}
