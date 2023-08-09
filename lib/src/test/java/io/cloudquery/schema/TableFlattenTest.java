package io.cloudquery.schema;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TableFlattenTest {

    public Table testTable = Table.builder().
            name("test").
            relations(List.of(
                    Table.builder().name("test2").build(),
                    Table.builder().name("test3").build(),
                    Table.builder().name("test4").build()
            )).build();

    @Test
    public void shouldFlattenTables() {
        List<Table> srcTables = List.of(testTable);
        List<Table> flattenedTables = Table.flattenTables(srcTables);

        assertEquals(1, srcTables.size());
        assertEquals(3, testTable.getRelations().size());
        assertEquals(4, flattenedTables.size());
    }

    @Test
    public void shouldFlattenTablesWithDuplicates() {
        List<Table> srcTables = List.of(testTable, testTable, testTable);
        List<Table> flattenedTables = Table.flattenTables(srcTables);

        assertEquals(3, srcTables.size());
        assertEquals(3, testTable.getRelations().size());
        assertEquals(4, flattenedTables.size());
    }
}
