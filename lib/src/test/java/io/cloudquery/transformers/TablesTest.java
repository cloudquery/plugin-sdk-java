package io.cloudquery.transformers;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.cloudquery.schema.Table;
import io.cloudquery.schema.Table.Transform;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TablesTest {

  @Mock private Transform child1Transformer;
  @Mock private Transform child1aTransformer, child1bTransformer;
  @Mock private Transform extraTransformer1, extraTransformer2;

  @Test
  void shouldSetTheParentOnACollectionOfTables() {
    List<Table> tables =
        List.of(
            Table.builder()
                .name("child1")
                .relations(
                    List.of(
                        Table.builder().name("child1a").build(),
                        Table.builder().name("child1b").build()))
                .build(),
            Table.builder()
                .name("child2")
                .relations(
                    List.of(
                        Table.builder().name("child2a").build(),
                        Table.builder().name("child2b").build()))
                .build());

    Tables.setParents(tables, Table.builder().name("parent").build());

    Map<String, Set<String>> tablesByParent = tablesByParent(tables);
    assertEquals(tablesByParent.get("parent"), Set.of("child1", "child2"));
    assertEquals(tablesByParent.get("child1"), Set.of("child1a", "child1b"));
    assertEquals(tablesByParent.get("child2"), Set.of("child2a", "child2b"));
  }

  @Test
  void shouldCallTransformOnEachTableIncludingRelations() throws TransformerException {
    List<Table> tables =
        List.of(
            Table.builder()
                .name("child1")
                .transform(child1Transformer)
                .relations(
                    List.of(
                        Table.builder().name("child1a").transform(child1aTransformer).build(),
                        Table.builder().name("child1b").transform(child1bTransformer).build()))
                .build());

    Tables.transformTables(tables);

    verify(child1Transformer, times(1)).transformTable(any());
    verify(child1aTransformer, times(1)).transformTable(any());
    verify(child1bTransformer, times(1)).transformTable(any());
  }

  @Test
  void shouldApplyExtraTransformationToTables() throws TransformerException {
    Table child1a = Table.builder().name("child1a").build();
    Table child1 = Table.builder().name("child1").relations(List.of(child1a)).build();

    Tables.apply(List.of(child1), List.of(extraTransformer1, extraTransformer2));

    verify(extraTransformer1, times(1)).transformTable(child1);
    verify(extraTransformer1, times(1)).transformTable(child1a);
    verify(extraTransformer2, times(1)).transformTable(child1);
    verify(extraTransformer2, times(1)).transformTable(child1a);
    verifyNoMoreInteractions(extraTransformer1, extraTransformer2);
  }

  private static Map<String, Set<String>> tablesByParent(List<Table> tables) {
    return Table.flattenTables(tables).stream()
        .collect(
            groupingBy(table -> table.getParent().getName(), mapping(Table::getName, toSet())));
  }
}
