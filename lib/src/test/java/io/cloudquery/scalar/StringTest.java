package io.cloudquery.scalar;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

public class StringTest {
  @Test
  public void testNew() {
    assertDoesNotThrow(
        () -> {
          new String();
        });
  }

  @Test
  public void testNewWithValidParam() {
    assertDoesNotThrow(
        () -> {
          new String(1);
          new String("PT8H6M12.345S");
          new String("");

          Scalar<?> s = new String(null);
          new String(s);
        });
  }

  @Test
  public void testToString() {
    String string = new String();
    assertEquals(Scalar.NULL_VALUE_STRING, string.toString());

    assertDoesNotThrow(
        () -> {
          string.set(1);
        });
    assertEquals("1", string.toString());

    assertDoesNotThrow(
        () -> {
          string.set(-1L);
        });
    assertEquals("-1", string.toString());
  }

  @Test
  public void testDataType() {
    String string = new String();
    assertEquals(ArrowType.Utf8.INSTANCE, string.dataType());
    assertEquals(new ArrowType.Utf8(), string.dataType());
  }

  @Test
  public void testIsValid() {
    String string = new String();
    assertFalse(string.isValid());

    assertDoesNotThrow(
        () -> {
          string.set(1L);
        });
    assertTrue(string.isValid());
  }

  @Test
  public void testSet() {
    String string = new String();
    assertDoesNotThrow(
        () -> {
          string.set(1);
          string.set(1L);
          string.set("PT8H6M12.345S");
          string.set(null);

          Scalar<?> s = new String("");
          string.set(s);
        });
  }

  @Test
  public void testGet() {
    String string = new String();
    assertFalse(string.isValid());
    assertNull(string.get());

    assertDoesNotThrow(
        () -> {
          string.set(-1L);
        });
    assertTrue(string.isValid());
    assertEquals("-1", string.get().toString());

    assertDoesNotThrow(
        () -> {
          string.set("");
        });
    assertTrue(string.isValid());
    assertEquals("", string.get().toString());
  }

  @Test
  public void testEquals() {
    String a = new String();
    String b = new String();
    assertEquals(a, b);
    assertNotEquals(a, null);
    assertNotEquals(a, new Bool()); // we can't cast Bool to String
    assertNotEquals(null, a);

    assertDoesNotThrow(
        () -> {
          a.set(-1L);
        });
    assertNotEquals(a, b);

    assertDoesNotThrow(
        () -> {
          for (Object obj : new Object[] {null, 0, 0L, -1, -1L, 1, 1L, "PT8H6M12.345S", ""}) {
            a.set(obj);
            assertEquals(a, new String(obj));
          }
        });
  }
}
