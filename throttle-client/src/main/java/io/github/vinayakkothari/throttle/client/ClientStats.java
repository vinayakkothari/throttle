package io.github.vinayakkothari.throttle.client;

/**
 * What the client has been doing, for dashboards and for the benchmark.
 *
 * @param decisions calls to {@code tryAcquire}
 * @param backendCalls round trips to the backend
 * @param servedFromLease decisions answered from locally leased tokens
 * @param expiredTokens leased tokens that went unused before the lease ran out
 * @param negativeCacheHits denials answered locally because the key was known to be exhausted
 * @param fallbacks decisions made by the fallback because the backend was unavailable
 */
public record ClientStats(
    long decisions,
    long backendCalls,
    long servedFromLease,
    long expiredTokens,
    long negativeCacheHits,
    long fallbacks,
    CircuitBreaker.State breaker) {

  /** Backend calls per 1,000 decisions. 1,000 means no saving at all. */
  public double callsPerThousand() {
    return decisions == 0 ? 0 : 1000.0 * backendCalls / decisions;
  }
}
