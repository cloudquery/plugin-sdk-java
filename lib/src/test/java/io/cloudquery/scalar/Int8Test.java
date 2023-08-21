package io.cloudquery.scalar;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

public class Int8Test {
  @Test
  public void testNew() {
    assertDoesNotThrow(
        () -> {
          new Number.Int8();
        });
  }

  @Test
  public void testNewWithValidParam() {
    assertDoesNotThrow(
        () -> {
          new Number.Int8(1);
          new Number.Int8("1");

          Scalar<?> s = new Number.Int8(2);
          new Number.Int8(s);
        });
  }

  @Test
  public void testNewWithInvalidParam() {
    assertThrows(
        ValidationException.class,
        () -> {
          new Number.Int8(new char[] {'q'});
        });
  }

  @Test
  public void testToString() {
    Number.Int8 int8 = new Number.Int8();
    assertEquals(Scalar.NULL_VALUE_STRING, int8.toString());

    assertDoesNotThrow(
        () -> {
          int8.set("1");
        });
    assertEquals("1", int8.toString());

    assertDoesNotThrow(
        () -> {
          int8.set(2);
        });
    assertEquals("2", int8.toString());
  }

  @Test
  public void testDataType() {
    Number.Int8 int8 = new Number.Int8();
    assertEquals(new ArrowType.Int(Byte.SIZE, true), int8.dataType());
  }

  @Test
  public void testIsValid() {
    Number.Int8 int8 = new Number.Int8();
    assertFalse(int8.isValid());

    assertDoesNotThrow(
        () -> {
          int8.set("1");
        });
    assertTrue(int8.isValid());
  }

  @Test
  public void testSet() {
    Number.Int8 int8 = new Number.Int8();
    assertDoesNotThrow(
        () -> {
          new Number.Int8(1);
          new Number.Int8("2");

          Scalar<?> s = new Number.Int8(1);
          int8.set(s);
        });
  }

  @Test
  public void testSetWithInvalidParam() {
    Number.Int8 int8 = new Number.Int8();
    assertThrows(
        ValidationException.class,
        () -> {
          int8.set(new char[] {});
        });
  }

  @Test
  public void testGet() {
    Number.Int8 int8 = new Number.Int8();
    assertFalse(int8.isValid());
    assertNull(int8.get());

    assertDoesNotThrow(
        () -> {
          int8.set(1);
        });
    assertTrue(int8.isValid());
    assertEquals((byte) 1, int8.get());

    assertDoesNotThrow(
        () -> {
          int8.set("-1");
        });
    assertTrue(int8.isValid());
    assertEquals((byte) -1, int8.get());
  }

  @Test
  public void testEquals() {
    Number.Int8 a = new Number.Int8();
    Number.Int8 b = new Number.Int8();
    assertEquals(a, b);
    assertNotEquals(a, null);
    assertNotEquals(a, new Binary()); // we can't cast Binary to Number.Int8
    assertNotEquals(null, a);

    assertDoesNotThrow(
        () -> {
          a.set(1);
        });
    assertNotEquals(a, b);

    assertDoesNotThrow(
        () -> {
          for (Object obj : new Object[] {null, 1, -1, "2"}) {
            a.set(obj);
            assertEquals(a, new Number.Int8(obj));
          }
        });
  }
}
