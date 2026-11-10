package io.github.vinayakkothari.throttle.core.redis;

/** Redis answered with an error reply, for example a script error or WRONGTYPE. */
public class RedisException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public RedisException(String message) {
    super(message);
  }

  public RedisException(String message, Throwable cause) {
    super(message, cause);
  }

  /** The first word of the error, like NOSCRIPT or WRONGTYPE. */
  public String code() {
    String m = getMessage();
    int space = m.indexOf(' ');
    return space < 0 ? m : m.substring(0, space);
  }
}
