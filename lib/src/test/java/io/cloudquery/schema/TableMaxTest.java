package io.cloudquery.schema;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TableMaxTest {

  public Table testTable =
      Table.builder()
          .name("test")
          .relations(
              List.of(
                  Table.builder().name("test2").build(),
                  Table.builder().name("test3").build(),
                  Table.builder().name("test4").build()))
          .build();

  @Test
  public void shouldReturnMaxDepth() {
    assertEquals(0, Table.maxDepth(Collections.emptyList()));
    assertEquals(2, Table.maxDepth(List.of(testTable)));
    assertEquals(
        3, Table.maxDepth(List.of(testTable.toBuilder().relations(List.of(testTable)).build())));
  }
}
