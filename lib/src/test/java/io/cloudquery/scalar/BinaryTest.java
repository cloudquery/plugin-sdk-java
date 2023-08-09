package io.cloudquery.scalar;

import io.cloudquery.scalar.Binary;
import io.cloudquery.scalar.ValidationException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;


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
            new Binary("abc");
        });
    }

    @Test
    public void testNewWithInvalidParam() {
        assertThrows(ValidationException.class, () -> {
            new Binary(false);
        });
    }
}
