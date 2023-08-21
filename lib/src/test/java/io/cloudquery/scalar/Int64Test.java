package io.cloudquery.scalar;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

public class Int64Test {
  @Test
  public void testNew() {
    assertDoesNotThrow(
        () -> {
          new Number.Int64();
        });
  }

  @Test
  public void testNewWithValidParam() {
    assertDoesNotThrow(
        () -> {
          new Number.Int64(1);
          new Number.Int64("1");

          Scalar<?> s = new Number.Int64(2);
          new Number.Int64(s);
        });
  }

  @Test
  public void testNewWithInvalidParam() {
    assertThrows(
        ValidationException.class,
        () -> {
          new Number.Int64(new char[] {'q'});
        });
  }

  @Test
  public void testToString() {
    Number.Int64 int64 = new Number.Int64();
    assertEquals(Scalar.NULL_VALUE_STRING, int64.toString());

    assertDoesNotThrow(
        () -> {
          int64.set("1");
        });
    assertEquals("1", int64.toString());

    assertDoesNotThrow(
        () -> {
          int64.set(2);
        });
    assertEquals("2", int64.toString());
  }

  @Test
  public void testDataType() {
    Number.Int64 int64 = new Number.Int64();
    assertEquals(new ArrowType.Int(Long.SIZE, true), int64.dataType());
  }

  @Test
  public void testIsValid() {
    Number.Int64 int64 = new Number.Int64();
    assertFalse(int64.isValid());

    assertDoesNotThrow(
        () -> {
          int64.set("1");
        });
    assertTrue(int64.isValid());
  }

  @Test
  public void testSet() {
    Number.Int64 int64 = new Number.Int64();
    assertDoesNotThrow(
        () -> {
          new Number.Int64(1);
          new Number.Int64("2");

          Scalar<?> s = new Number.Int64(1);
          int64.set(s);
        });
  }

  @Test
  public void testSetWithInvalidParam() {
    Number.Int64 int64 = new Number.Int64();
    assertThrows(
        ValidationException.class,
        () -> {
          int64.set(new char[] {});
        });
  }

  @Test
  public void testGet() {
    Number.Int64 int64 = new Number.Int64();
    assertFalse(int64.isValid());
    assertNull(int64.get());

    assertDoesNotThrow(
        () -> {
          int64.set(1);
        });
    assertTrue(int64.isValid());
    assertEquals((byte) 1, int64.get());

    assertDoesNotThrow(
        () -> {
          int64.set("-1");
        });
    assertTrue(int64.isValid());
    assertEquals((byte) -1, int64.get());
  }

  @Test
  public void testEquals() {
    Number.Int64 a = new Number.Int64();
    Number.Int64 b = new Number.Int64();
    assertEquals(a, b);
    assertNotEquals(a, null);
    assertNotEquals(a, new Binary()); // we can't cast Binary to Number.Int64
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
            assertEquals(a, new Number.Int64(obj));
          }
        });
  }
}
