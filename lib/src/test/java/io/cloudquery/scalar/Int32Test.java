package io.cloudquery.scalar;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

public class Int32Test {
  @Test
  public void testNew() {
    assertDoesNotThrow(
        () -> {
          new Number.Int32();
        });
  }

  @Test
  public void testNewWithValidParam() {
    assertDoesNotThrow(
        () -> {
          new Number.Int32(1);
          new Number.Int32("1");

          Scalar<?> s = new Number.Int32(2);
          new Number.Int32(s);
        });
  }

  @Test
  public void testNewWithInvalidParam() {
    assertThrows(
        ValidationException.class,
        () -> {
          new Number.Int32(new char[] {'q'});
        });
  }

  @Test
  public void testToString() {
    Number.Int32 int32 = new Number.Int32();
    assertEquals(Scalar.NULL_VALUE_STRING, int32.toString());

    assertDoesNotThrow(
        () -> {
          int32.set("1");
        });
    assertEquals("1", int32.toString());

    assertDoesNotThrow(
        () -> {
          int32.set(2);
        });
    assertEquals("2", int32.toString());
  }

  @Test
  public void testDataType() {
    Number.Int32 int32 = new Number.Int32();
    assertEquals(new ArrowType.Int(Integer.SIZE, true), int32.dataType());
  }

  @Test
  public void testIsValid() {
    Number.Int32 int32 = new Number.Int32();
    assertFalse(int32.isValid());

    assertDoesNotThrow(
        () -> {
          int32.set("1");
        });
    assertTrue(int32.isValid());
  }

  @Test
  public void testSet() {
    Number.Int32 int32 = new Number.Int32();
    assertDoesNotThrow(
        () -> {
          new Number.Int32(1);
          new Number.Int32("2");

          Scalar<?> s = new Number.Int32(1);
          int32.set(s);
        });
  }

  @Test
  public void testSetWithInvalidParam() {
    Number.Int32 int32 = new Number.Int32();
    assertThrows(
        ValidationException.class,
        () -> {
          int32.set(new char[] {});
        });
  }

  @Test
  public void testGet() {
    Number.Int32 int32 = new Number.Int32();
    assertFalse(int32.isValid());
    assertNull(int32.get());

    assertDoesNotThrow(
        () -> {
          int32.set(1);
        });
    assertTrue(int32.isValid());
    assertEquals((byte) 1, int32.get());

    assertDoesNotThrow(
        () -> {
          int32.set("-1");
        });
    assertTrue(int32.isValid());
    assertEquals((byte) -1, int32.get());
  }

  @Test
  public void testEquals() {
    Number.Int32 a = new Number.Int32();
    Number.Int32 b = new Number.Int32();
    assertEquals(a, b);
    assertNotEquals(a, null);
    assertNotEquals(a, new Binary()); // we can't cast Binary to Number.Int32
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
            assertEquals(a, new Number.Int32(obj));
          }
        });
  }
}
