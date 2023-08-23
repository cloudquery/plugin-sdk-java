package io.cloudquery.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cloudquery.schema.Table.TableBuilder;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TableColumnChangeTest {

  private TableBuilder tableBuilder;

  @BeforeEach
  public void setUp() {
    tableBuilder =
        Table.builder()
            .name("test")
            .columns(
                List.of(
                    Column.builder().name("basic_column").type(ArrowType.Bool.INSTANCE).build()));
  }

  @Test
  public void shouldBeNoChangesWithIdenticalTables() {
    Table old = tableBuilder.build();
    Table current = tableBuilder.build();

    List<TableColumnChange> changes = current.getChanges(old);

    assertEquals(0, changes.size(), "should be no changes");
  }

  @Test
  public void shouldAddColumnIfNewTableHasAdditionalColumn() {
    Table old = tableBuilder.build();
    Table current =
        tableBuilder
            .columns(
                List.of(
                    Column.builder().name("basic_column").type(ArrowType.Bool.INSTANCE).build(),
                    Column.builder()
                        .name("additional_column")
                        .type(ArrowType.Bool.INSTANCE)
                        .build()))
            .build();

    List<TableColumnChange> changes = current.getChanges(old);

    assertEquals(1, changes.size(), "should be 1 change");
    TableColumnChange change = changes.get(0);
    assertEquals(TableColumnChangeType.ADD, change.getType());
    assertEquals("additional_column", change.getColumnName());
    assertEquals("additional_column", change.getCurrent().getName());
  }

  @Test
  public void shouldRemoveColumnIfNewTableHasLessColumns() {
    Table old =
        Table.builder()
            .name("test")
            .columns(
                List.of(
                    Column.builder().name("basic_column").type(ArrowType.Bool.INSTANCE).build(),
                    Column.builder()
                        .name("additional_column")
                        .type(ArrowType.Bool.INSTANCE)
                        .build()))
            .build();

    Table current = tableBuilder.build();

    List<TableColumnChange> changes = current.getChanges(old);

    assertEquals(1, changes.size(), "should be 1 change");
    TableColumnChange change = changes.get(0);
    assertEquals(TableColumnChangeType.REMOVE, change.getType());
    assertEquals("additional_column", change.getColumnName());
    assertEquals("additional_column", change.getPrevious().getName());
  }

  @Test
  public void shouldUpdateColumnIfNewTableHasUpdateChange() {
    Table old = tableBuilder.build();
    Table current =
        tableBuilder
            .columns(
                List.of(
                    Column.builder().name("basic_column").type(ArrowType.Utf8.INSTANCE).build()))
            .build();

    List<TableColumnChange> changes = current.getChanges(old);

    assertEquals(1, changes.size(), "should be 1 change");
    TableColumnChange change = changes.get(0);
    assertEquals(TableColumnChangeType.UPDATE, change.getType());
    assertEquals("basic_column", change.getColumnName());
    assertEquals("basic_column", change.getCurrent().getName());
    assertEquals("basic_column", change.getPrevious().getName());
    assertEquals(ArrowType.Bool.INSTANCE, change.getPrevious().getType());
    assertEquals(ArrowType.Utf8.INSTANCE, change.getCurrent().getType());
  }
}
