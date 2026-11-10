package io.github.vinayakkothari.throttle.client;

import io.github.vinayakkothari.throttle.core.Decision;
import io.github.vinayakkothari.throttle.core.Rule;
import io.github.vinayakkothari.throttle.core.UnknownRuleException;
import java.util.List;

/**
 * Where decisions are made: the throttle service over HTTP, or Redis directly.
 *
 * <p>Implementations throw {@link BackendException} when they cannot get an answer, which is
 * what trips the circuit breaker, and {@link UnknownRuleException} when the rule does not exist,
 * which is the caller's mistake and never trips it.
 */
public interface Backend extends AutoCloseable {

  /** Take {@code want} units, or up to {@code want} when {@code partial} is true. */
  Decision acquire(String rule, String key, long want, boolean partial);

  /** Current rule definitions, used for local fallback limits. */
  List<Rule> rules();

  @Override
  default void close() {}
}
