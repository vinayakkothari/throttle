package io.github.vinayakkothari.throttle.client;

import io.github.vinayakkothari.throttle.core.Algorithm;
import io.github.vinayakkothari.throttle.core.Decision;
import io.github.vinayakkothari.throttle.core.FailMode;
import io.github.vinayakkothari.throttle.core.Rule;
import io.github.vinayakkothari.throttle.core.UnknownRuleException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * The library an app calls before doing rate limited work.
 *
 * <pre>{@code
 * ThrottleClient client = ThrottleClient.builder(HttpBackend.of("http://throttle:8080"))
 *     .leaseSize(20)
 *     .leaseTtl(Duration.ofMillis(100))
 *     .fallback(Fallback.local(4))
 *     .build();
 *
 * if (!client.tryAcquire("api", tenantId).allowed()) return tooManyRequests();
 * }</pre>
 *
 * <h2>Leasing</h2>
 *
 * With {@code leaseSize(n)} the client takes up to n tokens from the shared limit in one call
 * and hands them out locally, so most decisions cost no network call at all. Leased tokens
 * expire after {@code leaseTtl}. That bounds the error: a token taken now is spent within the
 * TTL, so over any window the shared limit is exceeded by at most {@code rate x TTL}. Tokens a
 * quiet server leases and never uses are wasted, which shows up as denials elsewhere. The
 * benchmark measures both effects.
 *
 * <h2>Negative cache</h2>
 *
 * When the backend says no and says when to retry, the client remembers that and answers no
 * locally until then. A client hammering an exhausted key costs the backend nothing.
 */
public final class ThrottleClient implements AutoCloseable {
  private static final Logger LOG = System.getLogger(ThrottleClient.class.getName());

  private final Backend backend;
  private final int leaseSize;
  private final long leaseTtlNanos;
  private final boolean negativeCache;
  private final long negativeMaxNanos;
  private final Fallback fallback;
  private final CircuitBreaker breaker;
  private final LongSupplier clock;

  private final Map<String, Slot> slots = new ConcurrentHashMap<>();
  private final Map<String, LocalBucket> local = new ConcurrentHashMap<>();
  private volatile Map<String, Rule> rules = Map.of();
  private final ScheduledExecutorService housekeeping;

  private final LongAdder decisions = new LongAdder();
  private final LongAdder backendCalls = new LongAdder();
  private final LongAdder servedFromLease = new LongAdder();
  private final LongAdder expiredTokens = new LongAdder();
  private final LongAdder negativeHits = new LongAdder();
  private final LongAdder fallbacks = new LongAdder();

  /** Per rule and key state. */
  private static final class Slot {
    final ReentrantLock lock = new ReentrantLock(); // ReentrantLock does not pin virtual threads
    long tokens;
    long expiresAt;
    long limit;
    volatile long deniedUntil;
    volatile long lastUsed;
  }

  private ThrottleClient(Builder b) {
    this.backend = b.backend;
    this.leaseSize = b.leaseSize;
    this.leaseTtlNanos = b.leaseTtl.toNanos();
    this.negativeCache = b.negativeCache;
    this.negativeMaxNanos = b.negativeCacheMax.toNanos();
    this.fallback = b.fallback;
    this.breaker = new CircuitBreaker(b.breakerThreshold, b.breakerOpenFor, b.clock);
    this.clock = b.clock;
    this.housekeeping =
        Executors.newSingleThreadScheduledExecutor(
            r -> Thread.ofPlatform().daemon().name("throttle-client").unstarted(r));
    housekeeping.execute(this::refreshRules);
    long refresh = b.ruleRefresh.toMillis();
    housekeeping.scheduleWithFixedDelay(this::refreshRules, refresh, refresh, TimeUnit.MILLISECONDS);
    housekeeping.scheduleWithFixedDelay(this::sweep, 5, 5, TimeUnit.SECONDS);
  }

  public static Builder builder(Backend backend) {
    return new Builder(backend);
  }

  public Decision tryAcquire(String rule, String key) {
    return tryAcquire(rule, key, 1);
  }

  public Decision tryAcquire(String rule, String key, long cost) {
    if (cost < 1) throw new IllegalArgumentException("cost must be at least 1");
    decisions.increment();
    long now = clock.getAsLong();
    Slot slot = slots.computeIfAbsent(rule + '\u0000' + key, k -> new Slot());
    slot.lastUsed = now;

    if (negativeCache && slot.deniedUntil > now) {
      negativeHits.increment();
      long waitMs = Math.max(1, (slot.deniedUntil - now) / 1_000_000);
      return Decision.deny(0, slot.limit, waitMs);
    }
    if (leaseSize <= 1) return direct(rule, key, cost, slot, now);
    return leased(rule, key, cost, slot);
  }

  /** No leasing: one backend call per decision, without holding any lock during the call. */
  private Decision direct(String rule, String key, long cost, Slot slot, long now) {
    Decision d = call(rule, key, cost, false);
    if (d == null) return fallback(rule, key, cost);
    slot.limit = d.limit();
    if (!d.allowed()) rememberDenial(slot, d, now);
    return d;
  }

  /**
   * Spend leased tokens, and refill when they run out. The slot lock is held during the refill,
   * so when many threads hit an empty slot one of them fetches and the rest wait for its answer
   * instead of all calling the backend at once.
   */
  private Decision leased(String rule, String key, long cost, Slot slot) {
    slot.lock.lock();
    try {
      long now = clock.getAsLong();
      if (slot.tokens > 0 && slot.expiresAt <= now) {
        expiredTokens.add(slot.tokens);
        slot.tokens = 0;
      }
      if (slot.tokens >= cost) {
        slot.tokens -= cost;
        servedFromLease.increment();
        return Decision.allow(cost, slot.tokens, slot.limit);
      }
      if (negativeCache && slot.deniedUntil > now) { // another thread just learned it is empty
        negativeHits.increment();
        return Decision.deny(0, slot.limit, Math.max(1, (slot.deniedUntil - now) / 1_000_000));
      }
      long want = Math.max(leaseSize, cost - slot.tokens);
      Decision d = call(rule, key, want, true);
      if (d == null) return fallback(rule, key, cost);
      now = clock.getAsLong();
      slot.limit = d.limit();
      slot.tokens += d.granted();
      slot.expiresAt = now + leaseTtlNanos;
      if (slot.tokens >= cost) {
        slot.tokens -= cost;
        return Decision.allow(cost, slot.tokens, d.limit());
      }
      rememberDenial(slot, d, now);
      return Decision.deny(d.remaining(), d.limit(), d.retryAfterMillis());
    } finally {
      slot.lock.unlock();
    }
  }

  private void rememberDenial(Slot slot, Decision d, long now) {
    if (!negativeCache) return;
    long wait = d.retryAfterMillis() < 0 ? negativeMaxNanos : d.retryAfterMillis() * 1_000_000;
    slot.deniedUntil = now + Math.min(wait, negativeMaxNanos);
  }

  /** One guarded backend call. Returns null when the backend could not answer. */
  private Decision call(String rule, String key, long want, boolean partial) {
    if (!breaker.tryPass()) return null;
    backendCalls.increment();
    try {
      Decision d = backend.acquire(rule, key, want, partial);
      breaker.onSuccess();
      return d;
    } catch (UnknownRuleException e) {
      breaker.onSuccess(); // the backend answered; the caller asked for the wrong thing
      throw e;
    } catch (BackendException e) {
      breaker.onFailure();
      return null;
    }
  }

  private Decision fallback(String ruleId, String key, long cost) {
    fallbacks.increment();
    Rule rule = rules.get(ruleId);
    // unknown rules fail open, matching the default fail mode of a rule
    return switch (fallback.mode()) {
      case ALLOW -> Decision.degraded(true, cost, 0);
      case DENY -> Decision.degraded(false, cost, 0);
      case RULE_DEFAULT ->
          Decision.degraded(
              rule == null || rule.failMode() == FailMode.OPEN,
              cost,
              rule == null ? 0 : rule.limitFor(key));
      case LOCAL -> {
        if (rule == null) yield Decision.degraded(true, cost, 0);
        LocalBucket bucket =
            local.computeIfAbsent(ruleId + '\u0000' + key, k -> localBucket(rule, key));
        yield Decision.degraded(bucket.tryTake(cost, clock.getAsLong()), cost, rule.limitFor(key));
      }
    };
  }

  private LocalBucket localBucket(Rule rule, String key) {
    int share = fallback.clusterSize();
    double capacity =
        (rule.algorithm() == Algorithm.TOKEN_BUCKET ? rule.capacityFor(key) : rule.limitFor(key))
            / (double) share;
    double ratePerNano = rule.limitFor(key) / (double) rule.period().toNanos() / share;
    return new LocalBucket(capacity, ratePerNano, clock.getAsLong());
  }

  private void refreshRules() {
    try {
      rules = backend.rules().stream().collect(Collectors.toUnmodifiableMap(Rule::id, r -> r));
    } catch (RuntimeException e) {
      LOG.log(Level.DEBUG, "could not refresh rules: {0}", e.getMessage());
    }
  }

  /** Forget keys nobody has used for a while, so a client limiting by IP does not grow forever. */
  private void sweep() {
    long now = clock.getAsLong();
    long idle = Math.max(leaseTtlNanos, negativeMaxNanos) + 30_000_000_000L;
    slots.entrySet().removeIf(e -> now - e.getValue().lastUsed > idle);
    if (breaker.state() == CircuitBreaker.State.CLOSED) local.clear();
  }

  public ClientStats stats() {
    return new ClientStats(
        decisions.sum(),
        backendCalls.sum(),
        servedFromLease.sum(),
        expiredTokens.sum(),
        negativeHits.sum(),
        fallbacks.sum(),
        breaker.state());
  }

  @Override
  public void close() {
    housekeeping.shutdownNow();
    backend.close();
  }

  public static final class Builder {
    private final Backend backend;
    private int leaseSize = 1;
    private Duration leaseTtl = Duration.ofMillis(100);
    private boolean negativeCache = true;
    private Duration negativeCacheMax = Duration.ofSeconds(1);
    private Fallback fallback = Fallback.ruleDefault();
    private int breakerThreshold = 5;
    private Duration breakerOpenFor = Duration.ofSeconds(1);
    private Duration ruleRefresh = Duration.ofSeconds(30);
    private LongSupplier clock = System::nanoTime;

    private Builder(Backend backend) {
      this.backend = backend;
    }

    /** Tokens fetched per backend call. 1 turns leasing off. */
    public Builder leaseSize(int n) {
      if (n < 1) throw new IllegalArgumentException("lease size must be at least 1");
      this.leaseSize = n;
      return this;
    }

    /** How long leased tokens stay valid. Bounds how far the shared limit can be overshot. */
    public Builder leaseTtl(Duration ttl) {
      this.leaseTtl = ttl;
      return this;
    }

    public Builder negativeCache(boolean on) {
      this.negativeCache = on;
      return this;
    }

    /** The longest a denial is remembered, so a raised limit is noticed quickly. */
    public Builder negativeCacheMax(Duration d) {
      this.negativeCacheMax = d;
      return this;
    }

    public Builder fallback(Fallback f) {
      this.fallback = f;
      return this;
    }

    public Builder circuitBreaker(int failures, Duration openFor) {
      this.breakerThreshold = failures;
      this.breakerOpenFor = openFor;
      return this;
    }

    public Builder ruleRefresh(Duration d) {
      this.ruleRefresh = d;
      return this;
    }

    Builder clock(LongSupplier c) {
      this.clock = c;
      return this;
    }

    public ThrottleClient build() {
      return new ThrottleClient(this);
    }
  }
}
