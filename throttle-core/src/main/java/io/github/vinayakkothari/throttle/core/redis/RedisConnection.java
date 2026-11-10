package io.github.vinayakkothari.throttle.core.redis;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import javax.net.ssl.SSLSocketFactory;

/** One TCP connection to Redis. Not thread safe: the pool hands it to one caller at a time. */
final class RedisConnection implements Closeable {
  private final Socket socket;
  private final InputStream in;
  private final OutputStream out;

  RedisConnection(RedisConfig config) throws IOException {
    Socket s =
        config.tls() ? SSLSocketFactory.getDefault().createSocket() : new Socket();
    try {
      s.setTcpNoDelay(true); // small request, small reply: never wait to batch
      s.setKeepAlive(true);
      int timeout = (int) config.timeout().toMillis();
      s.connect(new InetSocketAddress(config.host(), config.port()), timeout);
      s.setSoTimeout(timeout);
      this.socket = s;
      this.in = new BufferedInputStream(s.getInputStream(), 16 * 1024);
      this.out = new BufferedOutputStream(s.getOutputStream(), 16 * 1024);
      if (config.password() != null) {
        if (config.username() != null) call("AUTH", config.username(), config.password());
        else call("AUTH", config.password());
      }
      if (config.database() != 0) call("SELECT", Integer.toString(config.database()));
    } catch (IOException | RuntimeException e) {
      s.close();
      throw e;
    }
  }

  Object call(String... args) throws IOException {
    send(args);
    return Resp.read(in);
  }

  void send(String... args) throws IOException {
    Resp.writeCommand(out, args);
    out.flush();
  }

  /** Read the next pushed message; returns null if nothing arrived before the timeout. */
  Object readPush(int timeoutMillis) throws IOException {
    socket.setSoTimeout(timeoutMillis);
    try {
      return Resp.read(in);
    } catch (SocketTimeoutException e) {
      return null;
    }
  }

  @Override
  public void close() {
    try {
      socket.close();
    } catch (IOException ignored) {
      // closing anyway
    }
  }
}
