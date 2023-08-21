package io.cloudquery.scalar;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.joou.UInteger;
import org.junit.jupiter.api.Test;

public class UInt32Test {
  @Test
  public void testNew() {
    assertDoesNotThrow(
        () -> {
          new Number.UInt32();
        });
  }

  @Test
  public void testNewWithValidParam() {
    assertDoesNotThrow(
        () -> {
          new Number.UInt32(1);
          new Number.UInt32("1");

          Scalar<?> s = new Number.UInt32(2);
          new Number.UInt32(s);
        });
  }

  @Test
  public void testNewWithInvalidParam() {
    assertThrows(
        ValidationException.class,
        () -> {
          new Number.UInt32(new char[] {'q'});
        });
  }

  @Test
  public void testToString() {
    Number.UInt32 uint32 = new Number.UInt32();
    assertEquals(Scalar.NULL_VALUE_STRING, uint32.toString());

    assertDoesNotThrow(
        () -> {
          uint32.set("1");
        });
    assertEquals("1", uint32.toString());

    assertDoesNotThrow(
        () -> {
          uint32.set(2);
        });
    assertEquals("2", uint32.toString());
  }

  @Test
  public void testDataType() {
    Number.UInt32 uint32 = new Number.UInt32();
    assertEquals(new ArrowType.Int(Integer.SIZE, false), uint32.dataType());
  }

  @Test
  public void testIsValid() {
    Number.UInt32 uint32 = new Number.UInt32();
    assertFalse(uint32.isValid());

    assertDoesNotThrow(
        () -> {
          uint32.set("1");
        });
    assertTrue(uint32.isValid());
  }

  @Test
  public void testSet() {
    Number.UInt32 uint32 = new Number.UInt32();
    assertDoesNotThrow(
        () -> {
          new Number.UInt32(1);
          new Number.UInt32("2");

          Scalar<?> s = new Number.UInt32(1);
          uint32.set(s);
        });
  }

  @Test
  public void testSetWithInvalidParam() {
    Number.UInt32 uint32 = new Number.UInt32();
    assertThrows(
        ValidationException.class,
        () -> {
          uint32.set(new char[] {});
        });
  }

  @Test
  public void testGet() {
    Number.UInt32 uint32 = new Number.UInt32();
    assertFalse(uint32.isValid());
    assertNull(uint32.get());

    assertDoesNotThrow(
        () -> {
          uint32.set(1);
        });
    assertTrue(uint32.isValid());
    assertEquals(UInteger.valueOf(1), uint32.get());

    assertThrows(
        NumberFormatException.class,
        () -> {
          uint32.set("-1");
        });
  }

  @Test
  public void testEquals() {
    Number.UInt32 a = new Number.UInt32();
    Number.UInt32 b = new Number.UInt32();
    assertEquals(a, b);
    assertNotEquals(a, null);
    assertNotEquals(a, new Binary()); // we can't cast Binary to Number.UInt32
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
            assertEquals(a, new Number.UInt32(obj));
          }
        });
  }
}
