package io.cloudquery.server;

import io.cloudquery.server.AddressConverter.Address;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AddressTest {

    private AddressConverter addressConverter;

    @BeforeEach
    public void setUp() {
        addressConverter = new AddressConverter();
    }

    @Test
    public void shouldParseAddressFromString() throws Exception {
        String rawAddress = "127.0.0.1:12345";

        Address address = addressConverter.convert(rawAddress);

        assertEquals(new Address("127.0.0.1", 12345), address);
    }

    @Test
    public void shouldThrowExceptionIfAddressNotFormattedCorrectly() {
        String rawAddress = "bad address";

        AddressConverter addressConverter = new AddressConverter();

        assertThrows(AddressConverter.AddressParseException.class, () -> addressConverter.convert(rawAddress));
    }
}