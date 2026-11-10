package io.github.vinayakkothari.throttle.core;

import io.github.vinayakkothari.throttle.core.redis.RedisClient;
import io.github.vinayakkothari.throttle.core.redis.RedisException;
import io.github.vinayakkothari.throttle.core.redis.RedisSubscriber;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Rules live in one Redis hash and every server keeps a copy in memory, so a decision never
 * waits on a rule lookup.
 *
 * <p>A change is written to the hash and announced on a pub/sub channel, and every server
 * refreshes that one rule within milliseconds. Pub/sub drops messages while a subscriber is
 * disconnected, so each server also reloads everything on reconnect and on a slow timer. A
 * missed message costs at most one refresh interval, never a permanently stale rule.
 */
public final class RuleStore implements AutoCloseable {
  private static final Logger LOG = System.getLogger(RuleStore.class.getName());

  private final RedisClient redis;
  private final String hashKey;
  private final String channel;
  private final Map<String, Rule> cache = new ConcurrentHashMap<>();
  private RedisSubscriber subscriber;
  private ScheduledExecutorService timer;

  public RuleStore(RedisClient redis, String prefix) {
    this.redis = redis;
    this.hashKey = prefix + ":rules";
    this.channel = prefix + ":rules:changed";
  }

  /** Load everything, then follow changes as they happen. */
  public RuleStore start(Duration refreshEvery) {
    reload();
    subscriber = new RedisSubscriber(redis.config(), channel, this::refresh, this::reloadQuietly);
    subscriber.start();
    timer =
        Executors.newSingleThreadScheduledExecutor(
            r -> Thread.ofPlatform().daemon().name("rule-refresh").unstarted(r));
    long ms = refreshEvery.toMillis();
    timer.scheduleWithFixedDelay(this::reloadQuietly, ms, ms, TimeUnit.MILLISECONDS);
    return this;
  }

  public Optional<Rule> get(String id) {
    return Optional.ofNullable(cache.get(id));
  }

  public Collection<Rule> all() {
    return List.copyOf(cache.values());
  }

  public void put(Rule rule) {
    redis.call("HSET", hashKey, rule.id(), Json.write(rule.toJson()));
    cache.put(rule.id(), rule);
    redis.call("PUBLISH", channel, rule.id());
  }

  public boolean delete(String id) {
    long removed = (Long) redis.call("HDEL", hashKey, id);
    cache.remove(id);
    redis.call("PUBLISH", channel, id);
    return removed > 0;
  }

  /** Replace the whole cache with what Redis holds now. */
  public void reload() {
    List<?> flat = (List<?>) redis.call("HGETALL", hashKey);
    Map<String, Rule> fresh = new HashMap<>();
    for (int i = 0; i + 1 < flat.size(); i += 2) {
      String id = (String) flat.get(i);
      Rule rule = parse(id, (String) flat.get(i + 1));
      if (rule != null) fresh.put(id, rule);
    }
    cache.keySet().retainAll(fresh.keySet());
    cache.putAll(fresh);
  }

  /** One rule changed: fetch just that one. */
  private void refresh(String id) {
    try {
      Object raw = redis.call("HGET", hashKey, id);
      if (raw == null) {
        cache.remove(id);
      } else {
        Rule rule = parse(id, (String) raw);
        if (rule != null) cache.put(id, rule);
      }
    } catch (RedisException e) {
      LOG.log(Level.WARNING, "could not refresh rule {0}: {1}", id, e.getMessage());
    }
  }

  private void reloadQuietly() {
    try {
      reload();
    } catch (RedisException e) {
      LOG.log(Level.WARNING, "could not reload rules, keeping the last copy: {0}", e.getMessage());
    }
  }

  private static Rule parse(String id, String json) {
    try {
      return Rule.fromJson(Json.parseObject(json), id);
    } catch (IllegalArgumentException e) {
      LOG.log(Level.ERROR, "ignoring invalid rule {0}: {1}", id, e.getMessage());
      return null;
    }
  }

  @Override
  public void close() {
    if (subscriber != null) subscriber.close();
    if (timer != null) timer.shutdownNow();
  }
}
