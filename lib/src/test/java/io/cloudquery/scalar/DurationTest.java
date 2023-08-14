package io.cloudquery.scalar;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class DurationTest {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new Duration();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new Duration(1);
            new Duration("PT8H6M12.345S");
            new Duration(java.time.Duration.ZERO);
            new Duration(java.time.Duration.ofNanos(1));

            Scalar s = new Duration(java.time.Duration.ZERO);
            new Duration(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new Duration(false);
        });
    }

    @Test
    public void testToString() {
        Duration duration = new Duration();
        assertEquals(Scalar.NULL_VALUE_STRING, duration.toString());

        assertDoesNotThrow(() -> {
            duration.set(1);
        });
        assertEquals("PT0.001S", duration.toString());

        assertDoesNotThrow(() -> {
            duration.set(java.time.Duration.ofDays(1L));
        });
        assertEquals("PT24H", duration.toString());
    }

    @Test
    public void testDataType() {
        Duration duration = new Duration();
        assertEquals(new ArrowType.Duration(TimeUnit.MILLISECOND), duration.dataType());
    }

    @Test
    public void testIsValid() {
        Duration duration = new Duration();
        assertFalse(duration.isValid());

        assertDoesNotThrow(() -> {
            duration.set(1L);
        });
        assertTrue(duration.isValid());
    }

    @Test
    public void testSet() {
        Duration duration = new Duration();
        assertDoesNotThrow(() -> {
            duration.set(1);
            duration.set(1L);
            duration.set("PT8H6M12.345S");
            duration.set(java.time.Duration.ZERO);

            Scalar s = new Duration(java.time.Duration.ZERO);
            duration.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        Duration duration = new Duration();
        assertThrows(ValidationException.class, () -> {
            duration.set(false);
        });
    }

    @Test
    public void testGet() {
        Duration duration = new Duration();
        assertFalse(duration.isValid());
        assertNull(duration.get());

        assertDoesNotThrow(() -> {
            duration.set(-1L);
        });
        assertTrue(duration.isValid());
        assertEquals(java.time.Duration.ofMillis(-1L), duration.get());

        assertDoesNotThrow(() -> {
            duration.set(java.time.Duration.ZERO);
        });
        assertTrue(duration.isValid());
        assertEquals(java.time.Duration.ZERO, duration.get());
    }

    @Test
    public void testEquals() {
        Duration a = new Duration();
        Duration b = new Duration();
        assertEquals(a, b);
        assertNotEquals(a, null);
        assertNotEquals(a, new Bool()); // we can't cast Bool to Duration
        assertNotEquals(null, a);

        assertDoesNotThrow(() -> {
            a.set(-1L);
        });
        assertNotEquals(a, b);

        assertDoesNotThrow(() -> {
            for (Object obj : new Object[]{null, 0, 0L, -1, -1L, 1, 1L, "PT8H6M12.345S", java.time.Duration.ZERO}) {
                a.set(obj);
                assertEquals(a, new Duration(obj));
            }
        });
    }
}
