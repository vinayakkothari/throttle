package io.github.vinayakkothari.throttle.client;

import io.github.vinayakkothari.throttle.core.Decision;
import io.github.vinayakkothari.throttle.core.Rule;
import io.github.vinayakkothari.throttle.core.UnknownRuleException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** An in-memory backend with a fixed budget per key, that can be told to fail. */
final class FakeBackend implements Backend {
  final Map<String, Long> budget = new ConcurrentHashMap<>();
  final AtomicInteger calls = new AtomicInteger();
  final List<Rule> rules;
  final long limit;
  volatile boolean down;
  volatile long retryAfter = 500;

  FakeBackend(long limit, List<Rule> rules) {
    this.limit = limit;
    this.rules = rules;
  }

  @Override
  public Decision acquire(String rule, String key, long want, boolean partial) {
    calls.incrementAndGet();
    if (down) throw new BackendException("backend down", null);
    if (rule.equals("missing")) throw new UnknownRuleException(rule);
    synchronized (this) {
      long left = budget.getOrDefault(key, limit);
      long granted = left >= want ? want : partial ? left : 0;
      budget.put(key, left - granted);
      if (granted > 0) return Decision.allow(granted, left - granted, limit);
      return Decision.deny(left, limit, retryAfter);
    }
  }

  @Override
  public List<Rule> rules() {
    if (down) throw new BackendException("backend down", null);
    return rules;
  }
}
