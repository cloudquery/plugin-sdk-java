package io.cloudquery.scalar;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cloudquery.types.JSONType;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.junit.jupiter.api.Test;

public class JSONTest {
  @AllArgsConstructor
  public static class JsonData {
    public java.lang.String name;
    public int value;
  }

  @Test
  public void testNew() {
    assertDoesNotThrow(() -> new JSON());
  }

  @Test
  public void testNewWithValidParam() {
    assertDoesNotThrow(() -> new JSON("{}"));
    assertDoesNotThrow(() -> new JSON("{}".getBytes()));
    assertDoesNotThrow(() -> new JsonData("test", 1));
    assertDoesNotThrow(() -> new JSON(new JsonData("test", 1)));
    assertDoesNotThrow(() -> new JSON(new int[] {1, 2, 3}));
  }

  @Test
  public void testNewWithInvalidParam() {
    assertThrows(ValidationException.class, () -> new JSON("{\"name\":\"test\", incomplete"));
    assertThrows(
        ValidationException.class, () -> new JSON("{\"name\":\"test\", incomplete".getBytes()));
  }

  @Test
  public void testToString() throws ValidationException {
    JSON json = new JSON("{\"name\":\"test\", \"value\":1}");

    assertEquals("{\"name\":\"test\", \"value\":1}", json.toString());
  }

  @Test
  public void testDataType() {
    JSON json = new JSON();

    assertEquals(new JSONType(), json.dataType());
  }

  @Test
  public void testIsValid() throws ValidationException {
    assertTrue(new JSON("{}").isValid());

    assertTrue(new JSON("{\"name\":\"test\", \"value\":1}").isValid());
    assertTrue(new JSON(new int[] {1, 2, 3}).isValid());
    assertTrue(new JSON(Collections.emptyList()).isValid());
    assertTrue(new JSON(Map.of("foo", "bar")).isValid());
    assertTrue(new JSON(Collections.emptyMap()).isValid());
    assertTrue(new JSON(new String("{\"name\":\"test\", \"value\":1}")).isValid());

    assertFalse(new JSON("").isValid());
    assertFalse(new JSON(null).isValid());
    assertFalse(new JSON(new byte[] {}).isValid());
  }

  @Test
  public void testSet() throws ValidationException {
    JSON json = new JSON();

    json.set("{}");
    json.set(new JsonData("test", 1));
    json.set(new String("{\"name\":\"test\", \"value\":1}"));
    json.set(new int[] {1, 2, 3});
  }

  @Test
  public void testSetWithInvalidParam() {}

  @Test
  public void testGet() throws ValidationException {
    assertByteEquals("{}", new JSON("{}"));
    assertByteEquals("[1,2,3]", new JSON(new int[] {1, 2, 3}));
    assertByteEquals("[1,2,3]", new JSON(new int[] {1, 2, 3}));
    assertByteEquals("{\"name\":\"test\",\"value\":1}", new JSON(new JsonData("test", 1)));
    assertByteEquals("{\"foo\":\"bar\"}", new JSON(Map.of("foo", "bar")));
    assertByteEquals("{}", new JSON(Collections.emptyMap()));
    assertByteEquals("[]", new JSON(Collections.emptyList()));
  }

  @Test
  public void testEquals() throws ValidationException {
    JSON json1 = new JSON();
    JSON json2 = new JSON();

    assertEquals(json1, json2);
    assertNotEquals(json1, null);
    assertNotEquals(json1, new Bool());
    assertNotEquals(null, json1);

    json1.set(new JsonData("test", 1));
    assertNotEquals(json1, json2);
    json2.set(new JsonData("test", 1));
    assertEquals(json1, json2);
  }

  private void assertByteEquals(java.lang.String expected, JSON json) throws ValidationException {
    assertArrayEquals(
        expected.getBytes(),
        json.get(),
        "expected: " + expected + ", actual: " + new java.lang.String(json.get()));
  }
}
