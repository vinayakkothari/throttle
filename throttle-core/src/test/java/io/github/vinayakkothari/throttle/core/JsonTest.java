package io.github.vinayakkothari.throttle.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonTest {

  @Test
  void parsesEveryType() {
    Map<String, Object> m =
        Json.parseObject(
            "{\"s\":\"hi\",\"n\":42,\"neg\":-7,\"d\":1.5,\"e\":2e3,\"t\":true,\"f\":false,"
                + "\"z\":null,\"a\":[1,\"x\",[]],\"o\":{}}");
    assertEquals("hi", m.get("s"));
    assertEquals(42L, m.get("n"));
    assertEquals(-7L, m.get("neg"));
    assertEquals(1.5, m.get("d"));
    assertEquals(2000.0, m.get("e"));
    assertEquals(true, m.get("t"));
    assertEquals(false, m.get("f"));
    assertNull(m.get("z"));
    assertEquals(List.of(1L, "x", List.of()), m.get("a"));
    assertEquals(Map.of(), m.get("o"));
  }

  @Test
  void handlesEscapesBothWays() {
    String text = "quote \" back \\ tab \t newline \n unicode é ₹ control \u0001";
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("v", text);
    String json = Json.write(m);
    assertEquals(text, Json.parseObject(json).get("v"));
    assertEquals("A", Json.parse("\"\\u0041\""));
  }

  @Test
  void writesNumbersCleanly() {
    assertEquals("[1,2.5,3,null,true]", Json.write(Arrays.asList(1L, 2.5, 3.0, null, true)));
  }

  @Test
  void rejectsBadInput() {
    for (String bad :
        List.of("", "{", "{\"a\":}", "[1,]", "{\"a\":1} x", "\"unterminated", "{a:1}", "tru",
            "\"bad \\q escape\"", "-", "1-2")) {
      assertThrows(IllegalArgumentException.class, () -> Json.parse(bad), bad);
    }
  }

  @Test
  void refusesDeepNesting() {
    String deep = "[".repeat(100) + "]".repeat(100);
    assertThrows(IllegalArgumentException.class, () -> Json.parse(deep));
  }
}
