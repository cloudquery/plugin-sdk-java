package io.cloudquery.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TableTest {
  @Test
  public void shouldAddDefaultCQColumns() {
    Table table = Table.builder().name("test").build();

    assertEquals(0, table.getColumns().size());
    table.addCQIDs();

    List<Column> expectedColumns = List.of(Column.CQ_ID_COLUMN, Column.CQ_PARENT_ID_COLUMN);
    assertEquals(expectedColumns.size(), table.getColumns().size());
    for (int i = 0; i < expectedColumns.size(); i++) {
      assertEquals(expectedColumns.get(i).getName(), table.getColumns().get(i).getName());
    }
  }

  @Test
  public void shouldAddDefaultCQColumnsToRelations() {
    Table table = Table.builder().name("test").build();
    Table relation = Table.builder().name("relation").build();
    table.getRelations().add(relation);

    assertEquals(0, relation.getColumns().size());
    table.addCQIDs();

    List<Column> expectedColumns = List.of(Column.CQ_ID_COLUMN, Column.CQ_PARENT_ID_COLUMN);
    assertEquals(expectedColumns.size(), relation.getColumns().size());
    for (int i = 0; i < expectedColumns.size(); i++) {
      assertEquals(expectedColumns.get(i).getName(), relation.getColumns().get(i).getName());
    }
  }

  @Test
  public void shouldSetDefaultColumnAsPrimaryKeyIfNoOtherPrimaryKey() {
    Table table = Table.builder().name("test").build();

    table.addCQIDs();

    Optional<Column> column = table.getColumn(Column.CQ_ID_COLUMN.getName());
    assertTrue(column.isPresent());
    assertTrue(column.get().isPrimaryKey(), "CQ_ID_COLUMN should be primary key");
  }

  @Test
  public void shouldUseExistingPrimaryKeyWhenPossible() {
    Column pkColumn = Column.builder().name("mypk").primaryKey(true).build();
    Table table = Table.builder().name("test").columns(new ArrayList<>(List.of(pkColumn))).build();

    table.addCQIDs();

    Optional<Column> column = table.getColumn(Column.CQ_ID_COLUMN.getName());
    assertTrue(column.isPresent());
    assertFalse(column.get().isPrimaryKey(), "CQ_ID_COLUMN should not be primary key");
  }
}
