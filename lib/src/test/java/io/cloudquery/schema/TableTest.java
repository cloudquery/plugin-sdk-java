package io.cloudquery.schema;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TableTest {

    public Table testTable;

    @Before
    public void setUp() {
        testTable = Table.builder().
                name("test").
                relations(List.of(
                        Table.builder().name("test2").build(),
                        Table.builder().name("test3").build(),
                        Table.builder().name("test4").build()
                )).build();
    }

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

    @Test
    public void shouldReturnMaxDepth() {
        assertEquals(0, Table.maxDepth(Collections.emptyList()));
        assertEquals(2, Table.maxDepth(List.of(testTable)));
        assertEquals(3, Table.maxDepth(List.of(testTable.toBuilder().relations(List.of(testTable)).build())));
    }
}
