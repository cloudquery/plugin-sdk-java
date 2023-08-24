package io.cloudquery.schema;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.cloudquery.scalar.ValidationException;
import io.cloudquery.types.UUIDType;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

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

  @Test
  public void shouldResolveRandomCQId() throws ValidationException, NoSuchAlgorithmException {
    Table table = Table.builder().name("test").build();
    table.addCQIDs();

    Resource resource = Resource.builder().table(table).build();
    resource.resolveCQId(false);

    assertNotNull(resource.get(Column.CQ_ID_COLUMN.getName()).get());
    assertEquals(
        UUID.getClass().getName(),
        resource.get(Column.CQ_ID_COLUMN.getName()).get().getClass().getName());
  }

  @Test
  public void shouldResolveDeterministicCqId()
      throws ValidationException, NoSuchAlgorithmException {
    Column column1 =
        Column.builder().name("name").primaryKey(true).type(ArrowType.Utf8.INSTANCE).build();
    Column column2 =
        Column.builder().primaryKey(true).name("id").type(new ArrowType.Int(64, true)).build();
    Table table =
        Table.builder()
            .name("test")
            .columns(new ArrayList<Column>(Arrays.asList(column1, column2)))
            .build();
    table.addCQIDs();

    Resource resource = Resource.builder().table(table).build();
    resource.set(column1.getName(), "test");
    resource.set(column2.getName(), 1000);
    resource.resolveCQId(true);

    assertEquals(
        "a63a6152-e1d8-470f-f118-e5fa4874cb2d",
        resource.get(Column.CQ_ID_COLUMN.getName()).toString());
  }
}
