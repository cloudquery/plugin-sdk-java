package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class BoolTest {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new Bool();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new Bool(true);
            new Bool("true");

            Scalar s = new Bool(true);
            new Bool(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new Bool(new char[]{'1'});
        });
    }

    @Test
    public void testToString() {
        Bool b = new Bool();
        assertEquals(Scalar.NULL_VALUE_STRING, b.toString());

        assertDoesNotThrow(() -> {
            b.set(true);
        });
        assertEquals("true", b.toString());

        assertDoesNotThrow(() -> {
            b.set(false);
        });
        assertEquals("false", b.toString());
    }

    @Test
    public void testDataType() {
        Bool b = new Bool();
        assertEquals(ArrowType.Bool.INSTANCE, b.dataType());
        assertEquals(new ArrowType.Bool(), b.dataType());
    }

    @Test
    public void testIsValid() {
        Bool b = new Bool();
        assertFalse(b.isValid());

        assertDoesNotThrow(() -> {
            b.set("true");
        });
        assertTrue(b.isValid());
    }

    @Test
    public void testSet() {
        Bool b = new Bool();
        assertDoesNotThrow(() -> {
            new Bool(true);
            new Bool("true");

            Scalar s = new Bool(true);
            b.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        Bool b = new Bool();
        assertThrows(ValidationException.class, () -> {
            b.set(new char[]{});
        });
    }

    @Test
    public void testGet() {
        Bool b = new Bool();
        assertFalse(b.isValid());
        assertNull(b.get());

        assertDoesNotThrow(() -> {
            b.set(true);
        });
        assertTrue(b.isValid());
        assertEquals(true, b.get());

        assertDoesNotThrow(() -> {
            b.set("abc");
        });
        assertTrue(b.isValid());
        assertEquals(false, b.get());
    }

    @Test
    public void testEquals() {
        Bool a = new Bool();
        Bool b = new Bool();
        assertEquals(a, b);
        assertNotEquals(a, null);
        assertNotEquals(a, new Binary()); // we can't cast Binary to Bool
        assertNotEquals(null, a);

        assertDoesNotThrow(() -> {
            a.set(true);
        });
        assertNotEquals(a, b);

        assertDoesNotThrow(() -> {
            for (Object obj : new Object[]{null, true, false, "abc", "true", "false"}) {
                a.set(obj);
                assertEquals(a, new Bool(obj));
            }
        });
    }
}
