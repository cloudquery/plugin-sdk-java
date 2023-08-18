package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.joou.UByte;
import org.joou.UShort;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class UInt16Test {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new Number.UInt16();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new Number.UInt16(1);
            new Number.UInt16("1");

            Scalar<?> s = new Number.UInt16(2);
            new Number.UInt16(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new Number.UInt16(new char[]{'q'});
        });
    }

    @Test
    public void testToString() {
        Number.UInt16 uint16 = new Number.UInt16();
        assertEquals(Scalar.NULL_VALUE_STRING, uint16.toString());

        assertDoesNotThrow(() -> {
            uint16.set("1");
        });
        assertEquals("1", uint16.toString());

        assertDoesNotThrow(() -> {
            uint16.set(2);
        });
        assertEquals("2", uint16.toString());
    }

    @Test
    public void testDataType() {
        Number.UInt16 uint16 = new Number.UInt16();
        assertEquals(new ArrowType.Int(Short.SIZE, false), uint16.dataType());
    }

    @Test
    public void testIsValid() {
        Number.UInt16 uint16 = new Number.UInt16();
        assertFalse(uint16.isValid());

        assertDoesNotThrow(() -> {
            uint16.set("1");
        });
        assertTrue(uint16.isValid());
    }

    @Test
    public void testSet() {
        Number.UInt16 uint16 = new Number.UInt16();
        assertDoesNotThrow(() -> {
            new Number.UInt16(1);
            new Number.UInt16("2");

            Scalar<?> s = new Number.UInt16(1);
            uint16.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        Number.UInt16 uint16 = new Number.UInt16();
        assertThrows(ValidationException.class, () -> {
            uint16.set(new char[]{});
        });
    }

    @Test
    public void testGet() {
        Number.UInt16 uint16 = new Number.UInt16();
        assertFalse(uint16.isValid());
        assertNull(uint16.get());

        assertDoesNotThrow(() -> {
            uint16.set(1);
        });
        assertTrue(uint16.isValid());
        assertEquals(UShort.valueOf(1), uint16.get());

        assertThrows(NumberFormatException.class, () -> {
            uint16.set("-1");
        });
    }

    @Test
    public void testEquals() {
        Number.UInt16 a = new Number.UInt16();
        Number.UInt16 b = new Number.UInt16();
        assertEquals(a, b);
        assertNotEquals(a, null);
        assertNotEquals(a, new Binary()); // we can't cast Binary to Number.UInt16
        assertNotEquals(null, a);

        assertDoesNotThrow(() -> {
            a.set(1);
        });
        assertNotEquals(a, b);

        assertDoesNotThrow(() -> {
            for (Object obj : new Object[]{null, 1, -1, "2"}) {
                a.set(obj);
                assertEquals(a, new Number.UInt16(obj));
            }
        });
    }
}
