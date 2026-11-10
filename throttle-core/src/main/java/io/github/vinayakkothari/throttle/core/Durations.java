package io.github.vinayakkothari.throttle.core;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Short durations as people write them in config: 250ms, 1s, 5m, 1h, 1d. */
public final class Durations {
  private static final Pattern FORMAT = Pattern.compile("(\\d+)\\s*(ms|s|m|h|d)");

  private Durations() {}

  public static Duration parse(String text) {
    Matcher m = FORMAT.matcher(text.trim().toLowerCase());
    if (!m.matches()) {
      throw new IllegalArgumentException(
          "bad duration '" + text + "', use a number and ms, s, m, h or d, like 1s or 500ms");
    }
    long n = Long.parseLong(m.group(1));
    return switch (m.group(2)) {
      case "ms" -> Duration.ofMillis(n);
      case "s" -> Duration.ofSeconds(n);
      case "m" -> Duration.ofMinutes(n);
      case "h" -> Duration.ofHours(n);
      default -> Duration.ofDays(n);
    };
  }

  /** The largest whole unit: 60000 ms becomes "1m", 1500 ms stays "1500ms". */
  public static String format(Duration d) {
    long ms = d.toMillis();
    if (ms % 86_400_000 == 0) return ms / 86_400_000 + "d";
    if (ms % 3_600_000 == 0) return ms / 3_600_000 + "h";
    if (ms % 60_000 == 0) return ms / 60_000 + "m";
    if (ms % 1_000 == 0) return ms / 1_000 + "s";
    return ms + "ms";
  }
}
