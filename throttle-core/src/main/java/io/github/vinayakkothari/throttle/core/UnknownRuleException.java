package io.github.vinayakkothari.throttle.core;

/** The caller named a rule that does not exist. A caller bug, not an outage. */
public class UnknownRuleException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public UnknownRuleException(String ruleId) {
    super("no rule named '" + ruleId + "'");
  }
}
