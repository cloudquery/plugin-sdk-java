package io.cloudquery.scalar;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class Float32Test {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new Number.Float32();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new Number.Float32(1);
            new Number.Float32("1");

            Scalar<?> s = new Number.Float32(2);
            new Number.Float32(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new Number.Float32(new char[]{'q'});
        });
    }

    @Test
    public void testToString() {
        Number.Float32 float32 = new Number.Float32();
        assertEquals(Scalar.NULL_VALUE_STRING, float32.toString());

        assertDoesNotThrow(() -> {
            float32.set("1");
        });
        assertEquals("1.0", float32.toString());

        assertDoesNotThrow(() -> {
            float32.set(2);
        });
        assertEquals("2.0", float32.toString());
    }

    @Test
    public void testDataType() {
        Number.Float32 float32 = new Number.Float32();
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), float32.dataType());
    }

    @Test
    public void testIsValid() {
        Number.Float32 float32 = new Number.Float32();
        assertFalse(float32.isValid());

        assertDoesNotThrow(() -> {
            float32.set("1");
        });
        assertTrue(float32.isValid());
    }

    @Test
    public void testSet() {
        Number.Float32 float32 = new Number.Float32();
        assertDoesNotThrow(() -> {
            new Number.Float32(1);
            new Number.Float32("2");

            Scalar<?> s = new Number.Float32(1);
            float32.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        Number.Float32 float32 = new Number.Float32();
        assertThrows(ValidationException.class, () -> {
            float32.set(new char[]{});
        });
    }

    @Test
    public void testGet() {
        Number.Float32 float32 = new Number.Float32();
        assertFalse(float32.isValid());
        assertNull(float32.get());

        assertDoesNotThrow(() -> {
            float32.set(1);
        });
        assertTrue(float32.isValid());
        assertEquals(1, float32.get());

        assertDoesNotThrow(() -> {
            float32.set("-1");
        });
        assertTrue(float32.isValid());
        assertEquals(-1, float32.get());
    }

    @Test
    public void testEquals() {
        Number.Float32 a = new Number.Float32();
        Number.Float32 b = new Number.Float32();
        assertEquals(a, b);
        assertNotEquals(a, null);
        assertNotEquals(a, new Binary()); // we can't cast Binary to Number.Float32
        assertNotEquals(null, a);

        assertDoesNotThrow(() -> {
            a.set(1);
        });
        assertNotEquals(a, b);

        assertDoesNotThrow(() -> {
            for (Object obj : new Object[]{null, 1, -1, "2"}) {
                a.set(obj);
                assertEquals(a, new Number.Float32(obj));
            }
        });
    }
}
