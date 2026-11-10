package io.github.vinayakkothari.throttle.client;

/**
 * What the client does when the backend cannot answer.
 *
 * <ul>
 *   <li>{@code RULE_DEFAULT} use the rule's own fail mode, open or closed (the default)
 *   <li>{@code ALLOW} let everything through
 *   <li>{@code DENY} refuse everything
 *   <li>{@code LOCAL} keep limiting in memory, each app server taking its share of the limit
 * </ul>
 */
public record Fallback(Mode mode, int clusterSize) {
  public enum Mode {
    RULE_DEFAULT,
    ALLOW,
    DENY,
    LOCAL
  }

  public Fallback {
    if (mode == Mode.LOCAL && clusterSize < 1) {
      throw new IllegalArgumentException("local fallback needs the number of app servers");
    }
  }

  public static Fallback ruleDefault() {
    return new Fallback(Mode.RULE_DEFAULT, 0);
  }

  public static Fallback allow() {
    return new Fallback(Mode.ALLOW, 0);
  }

  public static Fallback deny() {
    return new Fallback(Mode.DENY, 0);
  }

  /**
   * Limit locally at {@code 1 / clusterSize} of each rule. With 4 app servers each one allows a
   * quarter of the limit, so the total stays close to the real limit while Redis is away.
   */
  public static Fallback local(int clusterSize) {
    return new Fallback(Mode.LOCAL, clusterSize);
  }
}
