package io.github.vinayakkothari.throttle.core;

import io.github.vinayakkothari.throttle.core.redis.RedisClient;
import io.github.vinayakkothari.throttle.core.redis.Script;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Runs a rule against Redis. Every decision is one Lua script call, so reading the current count
 * and updating it happen as one atomic step even with many servers calling at once.
 */
public final class Limiter {
  public static final long MAX_WANT = 100_000;

  private static final Script TOKEN_BUCKET = Script.resource(Limiter.class, "token_bucket.lua");
  private static final Script SLIDING_LOG =
      Script.resource(Limiter.class, "sliding_window_log.lua");
  private static final Script SLIDING_COUNTER =
      Script.resource(Limiter.class, "sliding_window_counter.lua");

  private final RedisClient redis;
  private final String prefix;

  public Limiter(RedisClient redis, String prefix) {
    this.redis = redis;
    this.prefix = prefix;
  }

  public Limiter(RedisClient redis) {
    this(redis, "throttle");
  }

  /** All or nothing: take {@code cost} units, or none. */
  public Decision check(Rule rule, String key, long cost) {
    return acquire(rule, key, cost, false);
  }

  /**
   * Take up to {@code want} units when {@code partial} is true. Clients use this to lease a batch
   * of tokens and spend them locally.
   */
  public Decision acquire(Rule rule, String key, long want, boolean partial) {
    validateKey(key);
    if (want < 1 || want > MAX_WANT) {
      throw new IllegalArgumentException("cost must be between 1 and " + MAX_WANT);
    }
    long limit = rule.limitFor(key);
    long period = rule.period().toMillis();
    String redisKey = redisKey(rule, key);
    String partialFlag = partial ? "1" : "0";
    String wantText = Long.toString(want);

    List<?> reply =
        (List<?>)
            switch (rule.algorithm()) {
              case TOKEN_BUCKET ->
                  redis.eval(
                      TOKEN_BUCKET,
                      List.of(redisKey),
                      List.of(
                          Long.toString(rule.capacityFor(key)),
                          plain(rule.ratePerMillisFor(key)),
                          wantText,
                          partialFlag));
              case SLIDING_WINDOW_LOG ->
                  redis.eval(
                      SLIDING_LOG,
                      List.of(redisKey),
                      List.of(
                          Long.toString(limit),
                          Long.toString(period),
                          wantText,
                          partialFlag,
                          Long.toHexString(ThreadLocalRandom.current().nextLong())));
              case SLIDING_WINDOW_COUNTER ->
                  redis.eval(
                      SLIDING_COUNTER,
                      List.of(redisKey),
                      List.of(Long.toString(limit), Long.toString(period), wantText, partialFlag));
            };
    long granted = (Long) reply.get(0);
    long remaining = (Long) reply.get(1);
    long retry = (Long) reply.get(2);
    if (granted > 0) return Decision.allow(granted, remaining, limit);
    return Decision.deny(remaining, limit, retry);
  }

  /**
   * Keys look like {@code throttle:api:tb:tenant-42}. The algorithm is part of the key so that
   * changing a rule's algorithm starts fresh instead of reading a hash as a sorted set.
   */
  public String redisKey(Rule rule, String key) {
    return prefix + ":" + rule.id() + ":" + rule.algorithm().shortName() + ":" + key;
  }

  /** Forget a key's state, for tests and for an admin reset. */
  public void reset(Rule rule, String key) {
    redis.call("DEL", redisKey(rule, key));
  }

  private static void validateKey(String key) {
    if (key == null || key.isEmpty()) throw new IllegalArgumentException("key is required");
    if (key.length() > Rule.MAX_KEY_LENGTH) {
      throw new IllegalArgumentException("key is longer than " + Rule.MAX_KEY_LENGTH);
    }
  }

  private static String plain(double d) {
    return BigDecimal.valueOf(d).toPlainString();
  }
}
