package io.cloudquery.scalar;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class DateDayTest {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new DateDay();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new DateDay(1);
            new DateDay("1");

            Scalar<?> s = new DateDay(2);
            new DateDay(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new DateDay(new char[]{'q'});
        });
    }

    @Test
    public void testToString() {
        DateDay dateDay = new DateDay();
        assertEquals(Scalar.NULL_VALUE_STRING, dateDay.toString());

        assertDoesNotThrow(() -> {
            dateDay.set("1");
        });
        assertEquals("1", dateDay.toString());

        assertDoesNotThrow(() -> {
            dateDay.set(2);
        });
        assertEquals("2", dateDay.toString());
    }

    @Test
    public void testDataType() {
        DateDay dateDay = new DateDay();
        assertEquals(new ArrowType.Date(DateUnit.DAY), dateDay.dataType());
    }

    @Test
    public void testIsValid() {
        DateDay dateDay = new DateDay();
        assertFalse(dateDay.isValid());

        assertDoesNotThrow(() -> {
            dateDay.set("1");
        });
        assertTrue(dateDay.isValid());
    }

    @Test
    public void testSet() {
        DateDay dateDay = new DateDay();
        assertDoesNotThrow(() -> {
            new DateDay(1);
            new DateDay("2");

            Scalar<?> s = new DateDay(1);
            dateDay.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        DateDay dateDay = new DateDay();
        assertThrows(ValidationException.class, () -> {
            dateDay.set(new char[]{});
        });
    }

    @Test
    public void testGet() {
        DateDay dateDay = new DateDay();
        assertFalse(dateDay.isValid());
        assertNull(dateDay.get());

        assertDoesNotThrow(() -> {
            dateDay.set(1);
        });
        assertTrue(dateDay.isValid());
        assertEquals(1, dateDay.get());

        assertDoesNotThrow(() -> {
            dateDay.set("-1");
        });
        assertTrue(dateDay.isValid());
        assertEquals(-1, dateDay.get());
    }

    @Test
    public void testEquals() {
        DateDay a = new DateDay();
        DateDay b = new DateDay();
        assertEquals(a, b);
        assertNotEquals(a, null);
        assertNotEquals(a, new Binary()); // we can't cast Binary to DateDay
        assertNotEquals(null, a);

        assertDoesNotThrow(() -> {
            a.set(1);
        });
        assertNotEquals(a, b);

        assertDoesNotThrow(() -> {
            for (Object obj : new Object[]{null, 1, -1, "2"}) {
                a.set(obj);
                assertEquals(a, new DateDay(obj));
            }
        });
    }
}
