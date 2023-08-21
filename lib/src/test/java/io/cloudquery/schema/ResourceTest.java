package io.cloudquery.schema;

import io.cloudquery.scalar.ValidationException;
import io.cloudquery.types.UUIDType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class ResourceTest {
    private static final UUID UUID = java.util.UUID.randomUUID();

    @Test
    public void shouldBuildWithNoErrors() {
        assertDoesNotThrow(() -> Resource.builder().table(Table.builder().name("").build()).build());
    }

    @Test
    public void shouldCreateScalarData() {
        Column column1 = Column.builder().name("test_column1").type(new UUIDType()).build();
        Column column2 = Column.builder().name("test_column2").type(ArrowType.Utf8.INSTANCE).build();
        Table table = Table.builder().name("test").columns(List.of(column1, column2)).build();

        Resource resource = Resource.builder().table(table).build();

        assertInstanceOf(io.cloudquery.scalar.UUID.class, resource.get(column1.getName()));
        assertInstanceOf(io.cloudquery.scalar.String.class, resource.get(column2.getName()));
    }

    @Test
    public void shouldSetAndGetDataTypes() throws ValidationException {
        Column column1 = Column.builder().name("test_column1").type(new UUIDType()).build();
        Table table = Table.builder().name("test").columns(List.of(column1)).build();

        Resource resource = Resource.builder().table(table).build();

        resource.set(column1.getName(), UUID);
        assertEquals(UUID, resource.get(column1.getName()).get());
    }
}
