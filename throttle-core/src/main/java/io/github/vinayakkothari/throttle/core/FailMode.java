package io.github.vinayakkothari.throttle.core;

/** What a rule does when Redis cannot be reached. */
public enum FailMode {
  /** Let traffic through. Right for limits that protect capacity, where an outage is worse. */
  OPEN,
  /** Refuse traffic. Right for limits that protect against abuse, like login attempts. */
  CLOSED;

  public String json() {
    return name().toLowerCase();
  }

  public static FailMode fromJson(String s) {
    return switch (s.toLowerCase()) {
      case "open" -> OPEN;
      case "closed" -> CLOSED;
      default -> throw new IllegalArgumentException("fail mode must be open or closed");
    };
  }
}
