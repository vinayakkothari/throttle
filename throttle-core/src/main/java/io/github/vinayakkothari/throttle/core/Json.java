package io.github.vinayakkothari.throttle.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Just enough JSON for small API bodies: objects, arrays, strings, numbers, booleans and null.
 *
 * <p>Objects become {@code LinkedHashMap<String, Object>}, arrays {@code ArrayList<Object>},
 * whole numbers {@code Long} and other numbers {@code Double}.
 */
public final class Json {
  private static final int MAX_DEPTH = 32;

  private Json() {}

  public static Object parse(String text) {
    Parser p = new Parser(text);
    p.skipSpace();
    Object value = p.value(0);
    p.skipSpace();
    if (p.pos != text.length()) throw p.error("unexpected trailing characters");
    return value;
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> parseObject(String text) {
    Object v = parse(text);
    if (!(v instanceof Map)) throw new IllegalArgumentException("expected a JSON object");
    return (Map<String, Object>) v;
  }

  public static String write(Object value) {
    StringBuilder sb = new StringBuilder();
    write(sb, value);
    return sb.toString();
  }

  private static void write(StringBuilder sb, Object v) {
    if (v == null) {
      sb.append("null");
    } else if (v instanceof String s) {
      quote(sb, s);
    } else if (v instanceof Boolean b) {
      sb.append(b);
    } else if (v instanceof Double d) {
      if (d.isNaN() || d.isInfinite()) throw new IllegalArgumentException("not a JSON number");
      sb.append(d == Math.rint(d) && Math.abs(d) < 1e15 ? Long.toString(d.longValue()) : d);
    } else if (v instanceof Float f) {
      write(sb, f.doubleValue());
    } else if (v instanceof Number n) {
      sb.append(n);
    } else if (v instanceof Map<?, ?> m) {
      sb.append('{');
      boolean first = true;
      for (Map.Entry<?, ?> e : m.entrySet()) {
        if (!first) sb.append(',');
        first = false;
        quote(sb, String.valueOf(e.getKey()));
        sb.append(':');
        write(sb, e.getValue());
      }
      sb.append('}');
    } else if (v instanceof Iterable<?> it) {
      sb.append('[');
      boolean first = true;
      for (Object o : it) {
        if (!first) sb.append(',');
        first = false;
        write(sb, o);
      }
      sb.append(']');
    } else if (v instanceof Enum<?> e) {
      quote(sb, e.name().toLowerCase());
    } else {
      throw new IllegalArgumentException("cannot write " + v.getClass().getName() + " as JSON");
    }
  }

  private static void quote(StringBuilder sb, String s) {
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        case '\b' -> sb.append("\\b");
        case '\f' -> sb.append("\\f");
        default -> {
          if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
          else sb.append(c);
        }
      }
    }
    sb.append('"');
  }

  private static final class Parser {
    private final String s;
    private int pos;

    Parser(String s) {
      this.s = s;
    }

    Object value(int depth) {
      if (depth > MAX_DEPTH) throw error("nested too deeply");
      if (pos >= s.length()) throw error("unexpected end of input");
      char c = s.charAt(pos);
      return switch (c) {
        case '{' -> object(depth);
        case '[' -> array(depth);
        case '"' -> string();
        case 't' -> literal("true", Boolean.TRUE);
        case 'f' -> literal("false", Boolean.FALSE);
        case 'n' -> literal("null", null);
        default -> {
          if (c == '-' || (c >= '0' && c <= '9')) yield number();
          throw error("unexpected character '" + c + "'");
        }
      };
    }

    Map<String, Object> object(int depth) {
      Map<String, Object> map = new LinkedHashMap<>();
      pos++; // {
      skipSpace();
      if (peek() == '}') {
        pos++;
        return map;
      }
      while (true) {
        skipSpace();
        if (peek() != '"') throw error("expected a string key");
        String key = string();
        skipSpace();
        expect(':');
        skipSpace();
        map.put(key, value(depth + 1));
        skipSpace();
        char c = next();
        if (c == '}') return map;
        if (c != ',') throw error("expected ',' or '}'");
      }
    }

    List<Object> array(int depth) {
      List<Object> list = new ArrayList<>();
      pos++; // [
      skipSpace();
      if (peek() == ']') {
        pos++;
        return list;
      }
      while (true) {
        skipSpace();
        list.add(value(depth + 1));
        skipSpace();
        char c = next();
        if (c == ']') return list;
        if (c != ',') throw error("expected ',' or ']'");
      }
    }

    String string() {
      pos++; // opening quote
      StringBuilder sb = new StringBuilder();
      while (true) {
        if (pos >= s.length()) throw error("unterminated string");
        char c = s.charAt(pos++);
        if (c == '"') return sb.toString();
        if (c < 0x20) throw error("control character in string");
        if (c != '\\') {
          sb.append(c);
          continue;
        }
        char e = next();
        switch (e) {
          case '"', '\\', '/' -> sb.append(e);
          case 'n' -> sb.append('\n');
          case 'r' -> sb.append('\r');
          case 't' -> sb.append('\t');
          case 'b' -> sb.append('\b');
          case 'f' -> sb.append('\f');
          case 'u' -> {
            if (pos + 4 > s.length()) throw error("bad unicode escape");
            try {
              sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
            } catch (NumberFormatException ex) {
              throw error("bad unicode escape");
            }
            pos += 4;
          }
          default -> throw error("bad escape \\" + e);
        }
      }
    }

    Object number() {
      int start = pos;
      if (peek() == '-') pos++;
      boolean fraction = false;
      while (pos < s.length()) {
        char c = s.charAt(pos);
        if (c >= '0' && c <= '9') {
          pos++;
        } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
          fraction = true;
          pos++;
        } else {
          break;
        }
      }
      String text = s.substring(start, pos);
      try {
        if (!fraction) return Long.parseLong(text);
        return Double.parseDouble(text);
      } catch (NumberFormatException e) {
        throw error("bad number " + text);
      }
    }

    Object literal(String word, Object value) {
      if (!s.startsWith(word, pos)) throw error("unexpected token");
      pos += word.length();
      return value;
    }

    void skipSpace() {
      while (pos < s.length()) {
        char c = s.charAt(pos);
        if (c != ' ' && c != '\n' && c != '\r' && c != '\t') return;
        pos++;
      }
    }

    char peek() {
      if (pos >= s.length()) throw error("unexpected end of input");
      return s.charAt(pos);
    }

    char next() {
      char c = peek();
      pos++;
      return c;
    }

    void expect(char c) {
      if (next() != c) throw error("expected '" + c + "'");
    }

    IllegalArgumentException error(String message) {
      return new IllegalArgumentException("invalid JSON at position " + pos + ": " + message);
    }
  }
}
