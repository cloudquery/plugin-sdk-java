package io.cloudquery.scalar;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UUIDTest {
    private static final byte[] COMPLETE_BYTE_SEQUENCE = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    private static final byte[] INCOMPLETE_BYTE_SEQUENCE = {1, 2, 3, 4};

    @Test
    public void testNew() {
        assertDoesNotThrow(() -> {
            new UUID();
        });
    }

    @Test
    public void testNewWithValidParam() {
        assertDoesNotThrow(() -> {
                    new UUID("123e4567-e89b-12d3-a456-426614174000");
                    new UUID(java.util.UUID.randomUUID());
                    new UUID(COMPLETE_BYTE_SEQUENCE);

                    Scalar<?> s = new UUID(java.util.UUID.randomUUID());
                    new UUID(s);
                }
        );
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new UUID(false);
        });

        assertThrows(ValidationException.class, () -> {
            new UUID(INCOMPLETE_BYTE_SEQUENCE);
        });
    }

    @Test
    public void testToString() {
        UUID uuid = new UUID();
        assertEquals(Scalar.NULL_VALUE_STRING, uuid.toString());

        java.util.UUID u = java.util.UUID.randomUUID();
        assertDoesNotThrow(() -> {
            uuid.set(u);
        });
        assertEquals(u.toString(), uuid.toString());

        assertDoesNotThrow(() -> {
            uuid.set(u.toString());
        });
        assertEquals(u.toString(), uuid.toString());
    }

    @Test
    public void testDataType() {
        UUID uuid = new UUID();
        assertEquals(new ArrowType.FixedSizeBinary(16), uuid.dataType());
    }

    @Test
    public void testIsValid() {
        UUID uuid = new UUID();
        assertFalse(uuid.isValid());

        assertDoesNotThrow(() -> {
            uuid.set(java.util.UUID.randomUUID());
        });
        assertTrue(uuid.isValid());
    }

    @Test
    public void testSet() {
        UUID uuid = new UUID();
        assertDoesNotThrow(() -> {
            uuid.set("123e4567-e89b-12d3-a456-426614174000");
            uuid.set(java.util.UUID.randomUUID());
            uuid.set(COMPLETE_BYTE_SEQUENCE);

            Scalar<?> s = new UUID(java.util.UUID.randomUUID());
            uuid.set(s);
        });
    }

    @Test
    public void testSetWithInvalidParam() {
        UUID uuid = new UUID();
        assertThrows(ValidationException.class, () -> {
            uuid.set(false);
        });
        assertThrows(ValidationException.class, () -> {
            uuid.set(INCOMPLETE_BYTE_SEQUENCE);
        });
    }

    @Test
    public void testGet() {
        UUID uuid = new UUID();
        assertFalse(uuid.isValid());
        assertNull(uuid.get());

        java.util.UUID u = java.util.UUID.randomUUID();
        assertDoesNotThrow(() -> {
            uuid.set(u);
        });
        assertTrue(uuid.isValid());
        assertEquals(u, uuid.get());
    }

    @Test
    public void testEquals() {
        UUID uuid1 = new UUID();
        UUID uuid2 = new UUID();

        assertEquals(uuid1, uuid2);
        assertNotEquals(uuid1, null);
        assertNotEquals(uuid1, new Bool());
        assertNotEquals(null, uuid1);

        assertDoesNotThrow(() -> {
            uuid1.set(java.util.UUID.randomUUID());
        });
        assertNotEquals(uuid1, uuid2);

        java.util.UUID u = java.util.UUID.randomUUID();
        assertDoesNotThrow(() -> {
            uuid1.set(u);
            assertEquals(uuid1, new UUID(u));
        });
    }

    @Test
    public void testCorrectEndianBehaviour() {
        String expectUUID = "00010203-0405-0607-0809-0a0b0c0d0e0f";

        UUID uuid = new UUID();
        assertDoesNotThrow(() -> {
            uuid.set(COMPLETE_BYTE_SEQUENCE);
            assertEquals(expectUUID, uuid.toString());
        });
    }

    @Test
    public void equalsContractVerification() {
        EqualsVerifier.forClass(UUID.class).
                suppress(Warning.NONFINAL_FIELDS). // Scalar<?> classes are intentionally mutable
                verify();
    }
}
