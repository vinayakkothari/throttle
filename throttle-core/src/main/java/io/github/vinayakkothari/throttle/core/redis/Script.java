package io.github.vinayakkothari.throttle.core.redis;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * A Lua script and its SHA-1. Redis caches scripts by SHA, so we send the 40 character hash on
 * every call and the full source only the first time a server has not seen it.
 */
public record Script(String name, String source, String sha) {

  public static Script of(String name, String source) {
    return new Script(name, source, sha1(source));
  }

  /** Load a script stored next to a class, for example {@code token_bucket.lua}. */
  public static Script resource(Class<?> owner, String file) {
    try (InputStream in = owner.getResourceAsStream(file)) {
      if (in == null) throw new IllegalStateException("missing script resource " + file);
      return of(file, new String(in.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String sha1(String source) {
    try {
      byte[] hash =
          MessageDigest.getInstance("SHA-1").digest(source.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
