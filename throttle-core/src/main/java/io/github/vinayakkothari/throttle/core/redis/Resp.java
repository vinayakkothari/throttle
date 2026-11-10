package io.github.vinayakkothari.throttle.core.redis;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The Redis wire protocol (RESP2), both directions.
 *
 * <p>Commands go out as an array of bulk strings. Replies come back as one of five types, which
 * map to Java like this:
 *
 * <pre>
 *   +OK          simple string  -> String
 *   -ERR ...     error          -> RedisException (thrown at the top level, kept inside arrays)
 *   :42          integer        -> Long
 *   $3 abc       bulk string    -> String, or null for $-1
 *   *2 ...       array          -> List&lt;Object&gt;, or null for *-1
 * </pre>
 */
public final class Resp {
  private static final byte[] CRLF = {'\r', '\n'};

  private Resp() {}

  public static void writeCommand(OutputStream out, String... args) throws IOException {
    out.write('*');
    out.write(ascii(args.length));
    out.write(CRLF);
    for (String arg : args) {
      byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
      out.write('$');
      out.write(ascii(bytes.length));
      out.write(CRLF);
      out.write(bytes);
      out.write(CRLF);
    }
  }

  /** Read one full reply. Error replies at the top level are thrown. */
  public static Object read(InputStream in) throws IOException {
    Object reply = readAny(in);
    if (reply instanceof RedisException e) throw e;
    return reply;
  }

  private static Object readAny(InputStream in) throws IOException {
    int type = in.read();
    if (type < 0) throw new EOFException("connection closed by redis");
    String line = readLine(in);
    switch (type) {
      case '+':
        return line;
      case '-':
        return new RedisException(line);
      case ':':
        return Long.parseLong(line);
      case '$':
        {
          int len = Integer.parseInt(line);
          if (len < 0) return null;
          byte[] data = in.readNBytes(len);
          if (data.length < len) throw new EOFException("short bulk string");
          expectCrlf(in);
          return new String(data, StandardCharsets.UTF_8);
        }
      case '*':
        {
          int n = Integer.parseInt(line);
          if (n < 0) return null;
          List<Object> items = new ArrayList<>(n);
          for (int i = 0; i < n; i++) items.add(readAny(in));
          return items;
        }
      default:
        throw new IOException("unexpected reply type '" + (char) type + "'");
    }
  }

  private static String readLine(InputStream in) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream(32);
    while (true) {
      int b = in.read();
      if (b < 0) throw new EOFException("connection closed mid-reply");
      if (b == '\r') {
        int next = in.read();
        if (next != '\n') throw new IOException("expected LF after CR");
        return buf.toString(StandardCharsets.UTF_8);
      }
      buf.write(b);
    }
  }

  private static void expectCrlf(InputStream in) throws IOException {
    if (in.read() != '\r' || in.read() != '\n') throw new IOException("expected CRLF");
  }

  private static byte[] ascii(int n) {
    return Integer.toString(n).getBytes(StandardCharsets.US_ASCII);
  }
}
