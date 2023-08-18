package io.cloudquery.scalar;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class DateMilliTest {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new DateMilli();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new DateMilli(1);
            new DateMilli("1");

            Scalar<?> s = new DateMilli(2);
            new DateMilli(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new DateMilli(new char[]{'q'});
        });
    }

    @Test
    public void testToString() {
        DateMilli dateMilli = new DateMilli();
        assertEquals(Scalar.NULL_VALUE_STRING, dateMilli.toString());

        assertDoesNotThrow(() -> {
            dateMilli.set("1");
        });
        assertEquals("1", dateMilli.toString());

        assertDoesNotThrow(() -> {
            dateMilli.set(2);
        });
        assertEquals("2", dateMilli.toString());
    }

    @Test
    public void testDataType() {
        DateMilli dateMilli = new DateMilli();
        assertEquals(new ArrowType.Date(DateUnit.MILLISECOND), dateMilli.dataType());
    }

    @Test
    public void testIsValid() {
        DateMilli dateMilli = new DateMilli();
        assertFalse(dateMilli.isValid());

        assertDoesNotThrow(() -> {
            dateMilli.set("1");
        });
        assertTrue(dateMilli.isValid());
    }

    @Test
    public void testSet() {
        DateMilli dateMilli = new DateMilli();
        assertDoesNotThrow(() -> {
            new DateMilli(1);
            new DateMilli("2");

            Scalar<?> s = new DateMilli(1);
            dateMilli.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        DateMilli dateMilli = new DateMilli();
        assertThrows(ValidationException.class, () -> {
            dateMilli.set(new char[]{});
        });
    }

    @Test
    public void testGet() {
        DateMilli dateMilli = new DateMilli();
        assertFalse(dateMilli.isValid());
        assertNull(dateMilli.get());

        assertDoesNotThrow(() -> {
            dateMilli.set(1);
        });
        assertTrue(dateMilli.isValid());
        assertEquals(1L, dateMilli.get());

        assertDoesNotThrow(() -> {
            dateMilli.set("-1");
        });
        assertTrue(dateMilli.isValid());
        assertEquals(-1L, dateMilli.get());
    }

    @Test
    public void testEquals() {
        DateMilli a = new DateMilli();
        DateMilli b = new DateMilli();
        assertEquals(a, b);
        assertNotEquals(a, null);
        assertNotEquals(a, new Binary()); // we can't cast Binary to DateMilli
        assertNotEquals(null, a);

        assertDoesNotThrow(() -> {
            a.set(1);
        });
        assertNotEquals(a, b);

        assertDoesNotThrow(() -> {
            for (Object obj : new Object[]{null, 1, -1, 0, 1L, -2L, "2"}) {
                a.set(obj);
                assertEquals(a, new DateMilli(obj));
            }
        });
    }
}
