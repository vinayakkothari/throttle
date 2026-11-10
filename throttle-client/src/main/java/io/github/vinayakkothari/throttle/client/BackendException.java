package io.github.vinayakkothari.throttle.client;

/** The backend could not give an answer: unreachable, timed out or failing. */
public class BackendException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public BackendException(String message, Throwable cause) {
    super(message, cause);
  }
}
