package io.cloudquery.schema;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TableMaxTest {

    public Table testTable = Table.builder().
            name("test").
            relations(List.of(
                    Table.builder().name("test2").build(),
                    Table.builder().name("test3").build(),
                    Table.builder().name("test4").build()
            )).build();

    @Test
    public void shouldReturnMaxDepth() {
        assertEquals(0, Table.maxDepth(Collections.emptyList()));
        assertEquals(2, Table.maxDepth(List.of(testTable)));
        assertEquals(3, Table.maxDepth(List.of(testTable.toBuilder().relations(List.of(testTable)).build())));
    }
}
