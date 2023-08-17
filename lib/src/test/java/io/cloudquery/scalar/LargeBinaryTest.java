package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class LargeBinaryTest {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new Binary.LargeBinary();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new Binary.LargeBinary(new byte[]{'a', 'b', 'c'});
            new Binary.LargeBinary("abc");
            new Binary.LargeBinary(new char[]{'a', 'b', 'c'});

            Scalar s = new Binary.LargeBinary(new char[]{'a', 'b', 'c'});
            new Binary.LargeBinary(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new Binary.LargeBinary(false);
        });
    }

    @Test
    public void testToString() {
        Binary.LargeBinary b = new Binary.LargeBinary();
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
        Binary.LargeBinary b = new Binary.LargeBinary();
        assertEquals(ArrowType.LargeBinary.INSTANCE, b.dataType());
        assertEquals(new ArrowType.LargeBinary(), b.dataType());
    }

    @Test
    public void testIsValid() {
        Binary.LargeBinary b = new Binary.LargeBinary();
        assertFalse(b.isValid());

        assertDoesNotThrow(() -> {
            b.set("abc");
        });
        assertTrue(b.isValid());
    }

    @Test
    public void testSet() {
        Binary.LargeBinary b = new Binary.LargeBinary();
        assertDoesNotThrow(() -> {
            b.set(new byte[]{'a', 'b', 'c'});
            b.set("abc");
            b.set(new char[]{'a', 'b', 'c'});

            Scalar s = new Binary.LargeBinary(new char[]{'a', 'b', 'c'});
            b.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        Binary.LargeBinary b = new Binary.LargeBinary();
        assertThrows(ValidationException.class, () -> {
            b.set(false);
        });
    }

    @Test
    public void testGet() {
        Binary.LargeBinary b = new Binary.LargeBinary();
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
            Scalar s = new Binary.LargeBinary(new char[]{'a', 'b', 'c'});
            b.set(s);
        });
        assertTrue(b.isValid());
        assertArrayEquals(new byte[]{105, -73}, (byte[]) b.get());

        assertDoesNotThrow(() -> {
            Scalar s = new Binary.LargeBinary(new byte[]{'a', 'b', 'c'});
            b.set(s);
        });
        assertTrue(b.isValid());
        assertArrayEquals(new byte[]{'a', 'b', 'c'}, (byte[]) b.get());
    }

    @Test
    public void testEquals() {
        Binary.LargeBinary a = new Binary.LargeBinary();
        Binary.LargeBinary b = new Binary.LargeBinary();
        assertEquals(a, b);
        assertNotEquals(a, null);
        assertNotEquals(a, new Bool()); // we can't cast Bool to Binary.LargeBinary
        assertNotEquals(null, a);

        assertDoesNotThrow(() -> {
            a.set(new byte[]{'a', 'b', 'c'});
        });
        assertNotEquals(a, b);

        assertDoesNotThrow(() -> {
            for (Object obj : new Object[]{
                    null,
                    new byte[]{'a', 'b', 'c'},
                    new char[]{'a', 'b', 'c'},
                    "abc",
                    new Binary.LargeBinary("abc"),
            }) {
                a.set(obj);
                assertEquals(a, new Binary.LargeBinary(obj));
            }
        });
    }
}
