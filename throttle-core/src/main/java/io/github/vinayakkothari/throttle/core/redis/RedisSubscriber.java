package io.github.vinayakkothari.throttle.core.redis;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.function.Consumer;

/**
 * Listens on one pub/sub channel on its own connection and reconnects when it drops.
 *
 * <p>Redis pub/sub is fire and forget: a message sent while we are disconnected is simply lost.
 * So every time the subscription is (re)established we call {@code onConnected}, which lets the
 * owner reload everything it might have missed.
 */
public final class RedisSubscriber implements AutoCloseable {
  private static final Logger LOG = System.getLogger(RedisSubscriber.class.getName());
  private static final int POLL_MILLIS = 5_000;

  private final RedisConfig config;
  private final String channel;
  private final Consumer<String> onMessage;
  private final Runnable onConnected;
  private final Thread thread;
  private volatile boolean running = true;
  private volatile RedisConnection current;

  public RedisSubscriber(
      RedisConfig config, String channel, Consumer<String> onMessage, Runnable onConnected) {
    this.config = config;
    this.channel = channel;
    this.onMessage = onMessage;
    this.onConnected = onConnected;
    this.thread = Thread.ofPlatform().daemon().name("redis-sub-" + channel).unstarted(this::loop);
  }

  public RedisSubscriber start() {
    thread.start();
    return this;
  }

  private void loop() {
    long backoff = 100;
    while (running) {
      try (RedisConnection conn = new RedisConnection(config)) {
        current = conn;
        conn.call("SUBSCRIBE", channel); // reply: ["subscribe", channel, 1]
        backoff = 100;
        safely(onConnected);
        int silent = 0;
        while (running) {
          Object push = conn.readPush(POLL_MILLIS);
          if (push == null) {
            // quiet channel: make sure the connection is still alive
            if (++silent > 1) throw new IOException("no reply to ping");
            conn.send("PING");
            continue;
          }
          silent = 0;
          if (push instanceof List<?> parts
              && parts.size() == 3
              && "message".equals(parts.get(0))) {
            String payload = String.valueOf(parts.get(2));
            safely(() -> onMessage.accept(payload));
          }
        }
      } catch (IOException | RedisException e) {
        if (!running) return;
        LOG.log(Level.WARNING, "subscription to {0} lost ({1}), retrying in {2} ms",
            channel, e.getMessage(), backoff);
        sleep(backoff);
        backoff = Math.min(backoff * 2, 5_000);
      }
    }
  }

  private static void safely(Runnable r) {
    try {
      r.run();
    } catch (RuntimeException e) {
      LOG.log(Level.WARNING, "subscriber callback failed", e);
    }
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    running = false;
    RedisConnection c = current;
    if (c != null) c.close();
    thread.interrupt();
  }
}
