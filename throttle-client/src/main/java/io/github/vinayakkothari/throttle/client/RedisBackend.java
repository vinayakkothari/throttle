package io.github.vinayakkothari.throttle.client;

import io.github.vinayakkothari.throttle.core.Decision;
import io.github.vinayakkothari.throttle.core.Limiter;
import io.github.vinayakkothari.throttle.core.Rule;
import io.github.vinayakkothari.throttle.core.RuleStore;
import io.github.vinayakkothari.throttle.core.UnknownRuleException;
import io.github.vinayakkothari.throttle.core.redis.RedisClient;
import io.github.vinayakkothari.throttle.core.redis.RedisException;
import java.time.Duration;
import java.util.List;

/**
 * Library mode: decide straight against Redis, with no throttle server in between. One less hop,
 * but every app now needs Redis credentials and runs the scripts itself.
 */
public final class RedisBackend implements Backend {
  private final RedisClient redis;
  private final RuleStore rules;
  private final Limiter limiter;

  public RedisBackend(RedisClient redis, String prefix) {
    this.redis = redis;
    this.rules = new RuleStore(redis, prefix).start(Duration.ofSeconds(30));
    this.limiter = new Limiter(redis, prefix);
  }

  public static RedisBackend connect(String redisUrl) {
    return new RedisBackend(RedisClient.create(redisUrl), "throttle");
  }

  @Override
  public Decision acquire(String rule, String key, long want, boolean partial) {
    Rule r = rules.get(rule).orElseThrow(() -> new UnknownRuleException(rule));
    try {
      return limiter.acquire(r, key, want, partial);
    } catch (RedisException e) {
      throw new BackendException(e.getMessage(), e);
    }
  }

  @Override
  public List<Rule> rules() {
    return List.copyOf(rules.all());
  }

  public RuleStore ruleStore() {
    return rules;
  }

  @Override
  public void close() {
    rules.close();
    redis.close();
  }
}
