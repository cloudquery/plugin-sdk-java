package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class LargeBinaryTest {
    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new LargeBinary();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
            new LargeBinary(new byte[]{'a', 'b', 'c'});
            new LargeBinary("abc");
            new LargeBinary(new char[]{'a', 'b', 'c'});

            Scalar s = new LargeBinary(new char[]{'a', 'b', 'c'});
            new LargeBinary(s);
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new LargeBinary(false);
        });
    }

    @Test
    public void testToString() {
        LargeBinary b = new LargeBinary();
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
        LargeBinary b = new LargeBinary();
        assertEquals(ArrowType.LargeBinary.INSTANCE, b.dataType());
        assertEquals(new ArrowType.LargeBinary(), b.dataType());
    }

    @Test
    public void testIsValid() {
        LargeBinary b = new LargeBinary();
        assertFalse(b.isValid());

        assertDoesNotThrow(() -> {
            b.set("abc");
        });
        assertTrue(b.isValid());
    }

    @Test
    public void testSet() {
        LargeBinary b = new LargeBinary();
        assertDoesNotThrow(() -> {
            b.set(new byte[]{'a', 'b', 'c'});
            b.set("abc");
            b.set(new char[]{'a', 'b', 'c'});

            Scalar s = new LargeBinary(new char[]{'a', 'b', 'c'});
            b.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        LargeBinary b = new LargeBinary();
        assertThrows(ValidationException.class, () -> {
            b.set(false);
        });
    }

    @Test
    public void testGet() {
        LargeBinary b = new LargeBinary();
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
            Scalar s = new LargeBinary(new char[]{'a', 'b', 'c'});
            b.set(s);
        });
        assertTrue(b.isValid());
        assertArrayEquals(new byte[]{105, -73}, (byte[]) b.get());

        assertDoesNotThrow(() -> {
            Scalar s = new LargeBinary(new byte[]{'a', 'b', 'c'});
            b.set(s);
        });
        assertTrue(b.isValid());
        assertArrayEquals(new byte[]{'a', 'b', 'c'}, (byte[]) b.get());
    }
    @Test
    public void testEquals() {
        LargeBinary a = new LargeBinary();
        LargeBinary b = new LargeBinary();
        assertEquals(a, b);
        assertNotEquals(a,null);
        assertNotEquals(a,new Binary()); // we can't cast Binary to LargeBinary
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
                    new LargeBinary("abc"),
            }) {
                a.set(obj);
                assertEquals(a, new LargeBinary(obj));
            }
        });
    }
}
