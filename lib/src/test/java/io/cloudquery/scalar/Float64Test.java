package io.cloudquery.scalar;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

public class Float64Test {
  @Test
  public void testNew() {
    assertDoesNotThrow(
        () -> {
          new Number.Float64();
        });
  }

  @Test
  public void testNewWithValidParam() {
    assertDoesNotThrow(
        () -> {
          new Number.Float64(1);
          new Number.Float64("1");

          Scalar<?> s = new Number.Float64(2);
          new Number.Float64(s);
        });
  }

  @Test
  public void testNewWithInvalidParam() {
    assertThrows(
        ValidationException.class,
        () -> {
          new Number.Float64(new char[] {'q'});
        });
  }

  @Test
  public void testToString() {
    Number.Float64 float64 = new Number.Float64();
    assertEquals(Scalar.NULL_VALUE_STRING, float64.toString());

    assertDoesNotThrow(
        () -> {
          float64.set("1");
        });
    assertEquals("1.0", float64.toString());

    assertDoesNotThrow(
        () -> {
          float64.set(2);
        });
    assertEquals("2.0", float64.toString());
  }

  @Test
  public void testDataType() {
    Number.Float64 float64 = new Number.Float64();
    assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), float64.dataType());
  }

  @Test
  public void testIsValid() {
    Number.Float64 float64 = new Number.Float64();
    assertFalse(float64.isValid());

    assertDoesNotThrow(
        () -> {
          float64.set("1");
        });
    assertTrue(float64.isValid());
  }

  @Test
  public void testSet() {
    Number.Float64 float64 = new Number.Float64();
    assertDoesNotThrow(
        () -> {
          new Number.Float64(1);
          new Number.Float64("2");

          Scalar<?> s = new Number.Float64(1);
          float64.set(s);
        });
  }

  @Test
  public void testSetWithInvalidParam() {
    Number.Float64 float64 = new Number.Float64();
    assertThrows(
        ValidationException.class,
        () -> {
          float64.set(new char[] {});
        });
  }

  @Test
  public void testGet() {
    Number.Float64 float64 = new Number.Float64();
    assertFalse(float64.isValid());
    assertNull(float64.get());

    assertDoesNotThrow(
        () -> {
          float64.set(1);
        });
    assertTrue(float64.isValid());
    assertEquals(1, float64.get());

    assertDoesNotThrow(
        () -> {
          float64.set("-1");
        });
    assertTrue(float64.isValid());
    assertEquals(-1, float64.get());
  }

  @Test
  public void testEquals() {
    Number.Float64 a = new Number.Float64();
    Number.Float64 b = new Number.Float64();
    assertEquals(a, b);
    assertNotEquals(a, null);
    assertNotEquals(a, new Binary()); // we can't cast Binary to Number.Float64
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
            assertEquals(a, new Number.Float64(obj));
          }
        });
  }
}
