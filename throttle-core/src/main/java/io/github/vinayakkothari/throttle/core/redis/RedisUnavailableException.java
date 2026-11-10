package io.github.vinayakkothari.throttle.core.redis;

/**
 * Redis could not be reached in time: connection refused, timed out or dropped. This is the
 * failure fail-open and fail-closed rules react to.
 */
public class RedisUnavailableException extends RedisException {
  private static final long serialVersionUID = 1L;

  public RedisUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
