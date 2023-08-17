package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.joou.UByte;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class UInt8Test {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new Number.UInt8();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new Number.UInt8(1);
            new Number.UInt8("1");

            Scalar<?> s = new Number.UInt8(2);
            new Number.UInt8(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new Number.UInt8(new char[]{'q'});
        });
    }

    @Test
    public void testToString() {
        Number.UInt8 uint8 = new Number.UInt8();
        assertEquals(Scalar.NULL_VALUE_STRING, uint8.toString());

        assertDoesNotThrow(() -> {
            uint8.set("1");
        });
        assertEquals("1", uint8.toString());

        assertDoesNotThrow(() -> {
            uint8.set(2);
        });
        assertEquals("2", uint8.toString());
    }

    @Test
    public void testDataType() {
        Number.UInt8 uint8 = new Number.UInt8();
        assertEquals(new ArrowType.Int(Byte.SIZE, false), uint8.dataType());
    }

    @Test
    public void testIsValid() {
        Number.UInt8 uint8 = new Number.UInt8();
        assertFalse(uint8.isValid());

        assertDoesNotThrow(() -> {
            uint8.set("1");
        });
        assertTrue(uint8.isValid());
    }

    @Test
    public void testSet() {
        Number.UInt8 uint8 = new Number.UInt8();
        assertDoesNotThrow(() -> {
            new Number.UInt8(1);
            new Number.UInt8("2");

            Scalar<?> s = new Number.UInt8(1);
            uint8.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        Number.UInt8 uint8 = new Number.UInt8();
        assertThrows(ValidationException.class, () -> {
            uint8.set(new char[]{});
        });
    }

    @Test
    public void testGet() {
        Number.UInt8 uint8 = new Number.UInt8();
        assertFalse(uint8.isValid());
        assertNull(uint8.get());

        assertDoesNotThrow(() -> {
            uint8.set(1);
        });
        assertTrue(uint8.isValid());
        assertEquals(UByte.valueOf(1), uint8.get());

        assertThrows(java.lang.NumberFormatException.class, () -> {
            uint8.set("-1");
        });
    }

    @Test
    public void testEquals() {
        Number.UInt8 a = new Number.UInt8();
        Number.UInt8 b = new Number.UInt8();
        assertEquals(a, b);
        assertNotEquals(a, null);
        assertNotEquals(a, new Binary()); // we can't cast Binary to Number.UInt8
        assertNotEquals(null, a);

        assertDoesNotThrow(() -> {
            a.set(1);
        });
        assertNotEquals(a, b);

        assertDoesNotThrow(() -> {
            for (Object obj : new Object[]{null, 1, -1, "2"}) {
                a.set(obj);
                assertEquals(a, new Number.UInt8(obj));
            }
        });
    }
}
