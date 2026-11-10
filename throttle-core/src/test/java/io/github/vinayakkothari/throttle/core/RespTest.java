package io.github.vinayakkothari.throttle.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vinayakkothari.throttle.core.redis.RedisException;
import io.github.vinayakkothari.throttle.core.redis.Resp;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class RespTest {

  private static Object read(String wire) throws Exception {
    return Resp.read(new ByteArrayInputStream(wire.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void writesCommandsAsBulkStrings() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Resp.writeCommand(out, "SET", "k", "₹");
    assertEquals("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\n₹\r\n", out.toString(StandardCharsets.UTF_8));
  }

  @Test
  void readsEachReplyType() throws Exception {
    assertEquals("OK", read("+OK\r\n"));
    assertEquals(42L, read(":42\r\n"));
    assertEquals("hello", read("$5\r\nhello\r\n"));
    assertEquals("", read("$0\r\n\r\n"));
    assertNull(read("$-1\r\n"));
    assertNull(read("*-1\r\n"));
    assertEquals(List.of(1L, "a", List.of(2L)), read("*3\r\n:1\r\n$1\r\na\r\n*1\r\n:2\r\n"));
  }

  @Test
  void throwsTopLevelErrorsAndKeepsNestedOnes() throws Exception {
    RedisException e = assertThrows(RedisException.class, () -> read("-NOSCRIPT no script\r\n"));
    assertEquals("NOSCRIPT", e.code());
    List<?> list = (List<?>) read("*2\r\n:1\r\n-ERR inside\r\n");
    assertTrue(list.get(1) instanceof RedisException);
  }

  @Test
  void failsOnTruncatedReplies() {
    assertThrows(EOFException.class, () -> read("$5\r\nhel"));
    assertThrows(EOFException.class, () -> read(""));
  }
}
