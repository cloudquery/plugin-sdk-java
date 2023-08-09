package io.cloudquery.scalar;

import io.cloudquery.scalar.Binary;
import io.cloudquery.scalar.ValidationException;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;


public class BinaryTest {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new Binary();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new Binary(new byte[]{'a', 'b', 'c'});
            new Binary("abc");
            new Binary(new char[]{'a', 'b', 'c'});

            Scalar s = new Binary(new char[]{'a', 'b', 'c'});
            new Binary(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new Binary(false);
        });
    }

    @Test
    public void testToString() {
        Binary b = new Binary();
        assertEquals(Scalar.NULL_VALUE_STRING, b.toString());

        assertDoesNotThrow(() -> {
            b.set("abc");
        });
        assertEquals("abc=", b.toString());

        assertDoesNotThrow(() -> {
            b.set(new byte[]{0, 1, 2, 3, 4, 5});
        });
        assertEquals("AAECAwQF", b.toString());
    }

    @Test
    public void testDataType() {
        Binary b = new Binary();
        assertEquals(ArrowType.Binary.INSTANCE, b.dataType());
        assertEquals(new ArrowType.Binary(), b.dataType());
    }

    @Test
    public void testIsValid() {
        Binary b = new Binary();
        assertFalse(b.isValid());

        assertDoesNotThrow(() -> {
            b.set("abc");
        });
        assertTrue(b.isValid());
    }

    @Test
    public void testSet() {
        Binary b = new Binary();
        assertDoesNotThrow(() -> {
            b.set(new byte[]{'a', 'b', 'c'});
            b.set("abc");
            b.set(new char[]{'a', 'b', 'c'});

            Scalar s = new Binary(new char[]{'a', 'b', 'c'});
            b.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        Binary b = new Binary();
        assertThrows(ValidationException.class, () -> {
            b.set(false);
        });
    }

    @Test
    public void testGet() {
        Binary b = new Binary();
        assertFalse(b.isValid());
        assertNull(b.get());

        assertDoesNotThrow(() -> {
            b.set(new byte[]{'a', 'b', 'c'});
        });
        assertTrue(b.isValid());
        assertArrayEquals(new byte[]{'a', 'b', 'c'}, (byte[]) b.get());

        assertDoesNotThrow(() -> {
            b.set("abc");
        });
        assertTrue(b.isValid());
        assertArrayEquals(new byte[]{105, -73}, (byte[]) b.get());

        assertDoesNotThrow(() -> {
            b.set(new char[]{'a', 'b', 'c'});
        });
        assertTrue(b.isValid());
        assertArrayEquals(new byte[]{105, -73}, (byte[]) b.get());

        assertDoesNotThrow(() -> {
            Scalar s = new Binary(new char[]{'a', 'b', 'c'});
            b.set(s);
        });
        assertTrue(b.isValid());
        assertArrayEquals(new byte[]{105, -73}, (byte[]) b.get());

        assertDoesNotThrow(() -> {
            Scalar s = new Binary(new byte[]{'a', 'b', 'c'});
            b.set(s);
        });
        assertTrue(b.isValid());
        assertArrayEquals(new byte[]{'a', 'b', 'c'}, (byte[]) b.get());
    }
    @Test
    public void testEquals() {
        Binary a = new Binary();
        Binary b = new Binary();
        assertEquals(a, b);
        assertNotEquals(a,null);
        assertNotEquals(null, a);

        assertDoesNotThrow(() -> {
            a.set(new byte[]{'a', 'b', 'c'});
        });
        assertNotEquals(a,b);

        assertDoesNotThrow(() -> {
            for (Object obj: new Object[]{
                    null,
                    new byte[]{'a', 'b', 'c'},
                    new char[]{'a', 'b', 'c'},
                    "abc",
                    new Binary("abc"),
            }) {
                a.set(obj);
                assertEquals(a, new Binary(obj));
            }
        });
    }
}
