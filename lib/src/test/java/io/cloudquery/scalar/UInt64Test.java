package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.joou.UInteger;
import org.joou.ULong;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class UInt64Test {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new Number.UInt64();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new Number.UInt64(1);
            new Number.UInt64("1");

            Scalar<?> s = new Number.UInt64(2);
            new Number.UInt64(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new Number.UInt64(new char[]{'q'});
        });
    }

    @Test
    public void testToString() {
        Number.UInt64 uint64 = new Number.UInt64();
        assertEquals(Scalar.NULL_VALUE_STRING, uint64.toString());

        assertDoesNotThrow(() -> {
            uint64.set("1");
        });
        assertEquals("1", uint64.toString());

        assertDoesNotThrow(() -> {
            uint64.set(2);
        });
        assertEquals("2", uint64.toString());
    }

    @Test
    public void testDataType() {
        Number.UInt64 uint64 = new Number.UInt64();
        assertEquals(new ArrowType.Int(Long.SIZE, false), uint64.dataType());
    }

    @Test
    public void testIsValid() {
        Number.UInt64 uint64 = new Number.UInt64();
        assertFalse(uint64.isValid());

        assertDoesNotThrow(() -> {
            uint64.set("1");
        });
        assertTrue(uint64.isValid());
    }

    @Test
    public void testSet() {
        Number.UInt64 uint64 = new Number.UInt64();
        assertDoesNotThrow(() -> {
            new Number.UInt64(1);
            new Number.UInt64("2");

            Scalar<?> s = new Number.UInt64(1);
            uint64.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        Number.UInt64 uint64 = new Number.UInt64();
        assertThrows(ValidationException.class, () -> {
            uint64.set(new char[]{});
        });
    }

    @Test
    public void testGet() {
        Number.UInt64 uint64 = new Number.UInt64();
        assertFalse(uint64.isValid());
        assertNull(uint64.get());

        assertDoesNotThrow(() -> {
            uint64.set(1);
        });
        assertTrue(uint64.isValid());
        assertEquals(ULong.valueOf(1), uint64.get());

        assertThrows(NumberFormatException.class, () -> {
            uint64.set("-1");
        });
    }

    @Test
    public void testEquals() {
        Number.UInt64 a = new Number.UInt64();
        Number.UInt64 b = new Number.UInt64();
        assertEquals(a, b);
        assertNotEquals(a, null);
        assertNotEquals(a, new Binary()); // we can't cast Binary to Number.UInt64
        assertNotEquals(null, a);

        assertDoesNotThrow(() -> {
            a.set(1);
        });
        assertNotEquals(a, b);

        assertDoesNotThrow(() -> {
            for (Object obj : new Object[]{null, 1, -1, "2"}) {
                a.set(obj);
                assertEquals(a, new Number.UInt64(obj));
            }
        });
    }
}
