package io.github.vinayakkothari.throttle.core.redis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A small, thread-safe Redis client backed by a pool of blocking connections.
 *
 * <p>Blocking I/O is a deliberate choice. Callers run on virtual threads, so a thread parked on
 * a socket read costs a few hundred bytes, not a megabyte of stack. That keeps this class short
 * enough to read in one sitting, with no event loop and no callbacks.
 */
public final class RedisClient implements AutoCloseable {
  private final RedisConfig config;
  private final Semaphore permits;
  private final ConcurrentLinkedDeque<RedisConnection> idle = new ConcurrentLinkedDeque<>();
  private final AtomicLong calls = new AtomicLong();
  private volatile boolean closed;

  public RedisClient(RedisConfig config) {
    this.config = config;
    this.permits = new Semaphore(config.poolSize(), true);
  }

  public static RedisClient create(String url) {
    return new RedisClient(RedisConfig.parse(url));
  }

  public RedisConfig config() {
    return config;
  }

  /** Commands sent so far, used by the benchmark to count round trips. */
  public long calls() {
    return calls.get();
  }

  /**
   * Run one command and return its reply.
   *
   * @throws RedisUnavailableException when Redis cannot be reached in time
   * @throws RedisException when Redis replies with an error
   */
  public Object call(String... args) {
    if (closed) throw new IllegalStateException("client is closed");
    acquirePermit();
    try {
      RedisConnection pooled = idle.pollFirst();
      if (pooled != null) {
        try {
          return callOn(pooled, args);
        } catch (IOException e) {
          // A pooled connection died while idle, most likely because Redis restarted. Its
          // siblings died the same way, so drop them all and try once on a fresh connection
          // rather than answer from the fail mode while Redis is in fact fine. If Redis did run
          // the command and only the reply was lost, the retry counts that request twice,
          // which errs on the side of limiting.
          drainIdle();
        }
      }
      try {
        return callOn(new RedisConnection(config), args);
      } catch (IOException e) {
        throw new RedisUnavailableException(
            "redis at " + config.describe() + " unavailable: " + e.getMessage(), e);
      }
    } finally {
      permits.release();
    }
  }

  /** Run one command; the connection goes back to the pool unless it broke. */
  private Object callOn(RedisConnection conn, String[] args) throws IOException {
    calls.incrementAndGet();
    try {
      Object reply = conn.call(args);
      idle.offerFirst(conn);
      return reply;
    } catch (RedisException e) {
      idle.offerFirst(conn); // a clean error reply leaves the connection usable
      throw e;
    } catch (IOException | RuntimeException e) {
      conn.close(); // broken mid-call, never reuse it
      throw e;
    }
  }

  private void drainIdle() {
    RedisConnection c;
    while ((c = idle.pollFirst()) != null) c.close();
  }

  /** Run a script by SHA, sending the source only if this Redis has not cached it yet. */
  public Object eval(Script script, List<String> keys, List<String> args) {
    List<String> cmd = new ArrayList<>(3 + keys.size() + args.size());
    cmd.add("EVALSHA");
    cmd.add(script.sha());
    cmd.add(Integer.toString(keys.size()));
    cmd.addAll(keys);
    cmd.addAll(args);
    try {
      return call(cmd.toArray(String[]::new));
    } catch (RedisException e) {
      if (!"NOSCRIPT".equals(e.code())) throw e;
      cmd.set(0, "EVAL");
      cmd.set(1, script.source());
      return call(cmd.toArray(String[]::new));
    }
  }

  public boolean ping() {
    try {
      return "PONG".equals(call("PING"));
    } catch (RedisException e) {
      return false;
    }
  }

  private void acquirePermit() {
    try {
      if (!permits.tryAcquire(config.timeout().toMillis(), TimeUnit.MILLISECONDS)) {
        throw new RedisUnavailableException(
            "no free redis connection within " + config.timeout().toMillis() + " ms", null);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RedisUnavailableException("interrupted waiting for a redis connection", e);
    }
  }

  @Override
  public void close() {
    closed = true;
    drainIdle();
  }
}
