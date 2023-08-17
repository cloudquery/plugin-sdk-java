package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class Int16Test {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new Number.Int16();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new Number.Int16(1);
            new Number.Int16("1");

            Scalar<?> s = new Number.Int16(2);
            new Number.Int16(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new Number.Int16(new char[]{'q'});
        });
    }

    @Test
    public void testToString() {
        Number.Int16 int16 = new Number.Int16();
        assertEquals(Scalar.NULL_VALUE_STRING, int16.toString());

        assertDoesNotThrow(() -> {
            int16.set("1");
        });
        assertEquals("1", int16.toString());

        assertDoesNotThrow(() -> {
            int16.set(2);
        });
        assertEquals("2", int16.toString());
    }

    @Test
    public void testDataType() {
        Number.Int16 int16 = new Number.Int16();
        assertEquals(new ArrowType.Int(Short.SIZE, true), int16.dataType());
    }

    @Test
    public void testIsValid() {
        Number.Int16 int16 = new Number.Int16();
        assertFalse(int16.isValid());

        assertDoesNotThrow(() -> {
            int16.set("1");
        });
        assertTrue(int16.isValid());
    }

    @Test
    public void testSet() {
        Number.Int16 int16 = new Number.Int16();
        assertDoesNotThrow(() -> {
            new Number.Int16(1);
            new Number.Int16("2");

            Scalar<?> s = new Number.Int16(1);
            int16.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        Number.Int16 int16 = new Number.Int16();
        assertThrows(ValidationException.class, () -> {
            int16.set(new char[]{});
        });
    }

    @Test
    public void testGet() {
        Number.Int16 int16 = new Number.Int16();
        assertFalse(int16.isValid());
        assertNull(int16.get());

        assertDoesNotThrow(() -> {
            int16.set(1);
        });
        assertTrue(int16.isValid());
        assertEquals((byte) 1, int16.get());

        assertDoesNotThrow(() -> {
            int16.set("-1");
        });
        assertTrue(int16.isValid());
        assertEquals((byte) -1, int16.get());
    }

    @Test
    public void testEquals() {
        Number.Int16 a = new Number.Int16();
        Number.Int16 b = new Number.Int16();
        assertEquals(a, b);
        assertNotEquals(a, null);
        assertNotEquals(a, new Binary()); // we can't cast Binary to Number.Int16
        assertNotEquals(null, a);

        assertDoesNotThrow(() -> {
            a.set(1);
        });
        assertNotEquals(a, b);

        assertDoesNotThrow(() -> {
            for (Object obj : new Object[]{null, 1, -1, "2"}) {
                a.set(obj);
                assertEquals(a, new Number.Int16(obj));
            }
        });
    }
}
