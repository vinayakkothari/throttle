package io.github.vinayakkothari.throttle.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A TCP proxy in front of Redis that a test can cut and restore, to watch the service behave
 * while Redis is unreachable without stopping the real Redis.
 */
final class CuttableProxy implements AutoCloseable {
  private final ServerSocket listener;
  private final String targetHost;
  private final int targetPort;
  private final Set<Socket> open = ConcurrentHashMap.newKeySet();
  private volatile boolean cut;
  private volatile boolean closed;

  CuttableProxy(String redisUrl) throws IOException {
    URI uri = URI.create(redisUrl);
    this.targetHost = uri.getHost();
    this.targetPort = uri.getPort() == -1 ? 6379 : uri.getPort();
    this.listener = new ServerSocket();
    listener.setReuseAddress(true);
    listener.bind(new InetSocketAddress("127.0.0.1", 0));
    Thread.ofVirtual().start(this::acceptLoop);
  }

  String url() {
    return "redis://127.0.0.1:" + listener.getLocalPort();
  }

  /** Drop every connection and refuse new ones until restored. */
  void cut() {
    cut = true;
    for (Socket s : open) closeQuietly(s);
    open.clear();
  }

  void restore() {
    cut = false;
  }

  private void acceptLoop() {
    while (!closed) {
      try {
        Socket client = listener.accept();
        if (cut) {
          closeQuietly(client);
          continue;
        }
        Socket upstream = new Socket(targetHost, targetPort);
        open.add(client);
        open.add(upstream);
        Thread.ofVirtual().start(() -> pump(client, upstream));
        Thread.ofVirtual().start(() -> pump(upstream, client));
      } catch (IOException e) {
        if (closed) return;
      }
    }
  }

  private void pump(Socket from, Socket to) {
    byte[] buf = new byte[8192];
    try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
      int n;
      while ((n = in.read(buf)) >= 0) {
        out.write(buf, 0, n);
        out.flush();
      }
    } catch (IOException ignored) {
      // one side went away
    } finally {
      closeQuietly(from);
      closeQuietly(to);
      open.remove(from);
      open.remove(to);
    }
  }

  private static void closeQuietly(Socket s) {
    try {
      s.close();
    } catch (IOException ignored) {
      // already closed
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
    cut();
    listener.close();
  }
}
