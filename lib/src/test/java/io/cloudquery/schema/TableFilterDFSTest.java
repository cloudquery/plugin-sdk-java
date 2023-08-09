package io.cloudquery.schema;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class TableFilterDFSTest {
    public static final List<Table> BASIC_TABLES = Stream.of("table1", "table2", "table3").map(
            name -> Table.builder().name(name).build()
    ).toList();

    public static final List<Table> NESTED_TABLE = List.of(
            Table.builder().name("main_table").relations(
                    List.of(
                            Table.builder().name("sub_table").relations(
                                    List.of(
                                            Table.builder().name("sub_sub_table").build()
                                    )
                            ).build()
                    )
            ).build()
    );

    public static final List<String> EMPTY_CONFIGURATION = Collections.emptyList();

    @Test
    public void shouldReturnAllTables() throws SchemaException {
        List<String> includeConfiguration = List.of("*");

        List<Table> filteredTables = Table.filterDFS(BASIC_TABLES, includeConfiguration, EMPTY_CONFIGURATION, false);

        assertThat(extractTableNames(filteredTables)).containsOnly("table1", "table2", "table3");
    }

    @Test
    public void shouldFilterTables() throws SchemaException {
        List<String> includeConfiguration = List.of("*");
        List<String> skipConfiguration = List.of("table1", "table3");

        List<Table> filteredTables = Table.filterDFS(BASIC_TABLES, includeConfiguration, skipConfiguration, false);

        assertThat(extractTableNames(filteredTables)).containsOnly("table2");
    }

    @Test
    public void shouldFilterSpecificTableWhenProvided() throws SchemaException {
        List<String> includeConfiguration = List.of("table2");

        List<Table> filteredTables = Table.filterDFS(BASIC_TABLES, includeConfiguration, EMPTY_CONFIGURATION, false);

        assertThat(extractTableNames(filteredTables)).containsOnly("table2");
    }

    @Test
    public void shouldFilterTablesMatchingGlobPattern() throws SchemaException {
        List<String> includeConfiguration = List.of("table*");
        List<String> skipConfiguration = List.of("table2", "table3");

        List<Table> filteredTables = Table.filterDFS(BASIC_TABLES, includeConfiguration, skipConfiguration, false);

        assertThat(extractTableNames(filteredTables)).containsOnly("table1");
    }

    @Test
    public void shouldReturnTableOnlyOnceEvenIfMatchedByMultiplePatterns() throws SchemaException {
        List<String> includeConfiguration = List.of("*", "table1", "table*", "table2");

        List<Table> filteredTables = Table.filterDFS(BASIC_TABLES, includeConfiguration, EMPTY_CONFIGURATION, false);

        assertThat(extractTableNames(filteredTables)).containsOnly("table1", "table2", "table3");
    }

    @Test
    public void shouldMatchPrefixGlobs() throws SchemaException {
        List<String> includeConfiguration = List.of("*2");

        List<Table> filteredTables = Table.filterDFS(BASIC_TABLES, includeConfiguration, EMPTY_CONFIGURATION, false);

        assertThat(extractTableNames(filteredTables)).containsOnly("table2");
    }

    @Test
    public void shouldMatchSuffixGlobs() throws SchemaException {
        List<String> includeConfiguration = List.of("table*");

        List<Table> filteredTables = Table.filterDFS(BASIC_TABLES, includeConfiguration, EMPTY_CONFIGURATION, false);

        assertThat(extractTableNames(filteredTables)).containsOnly("table1", "table2", "table3");
    }

    @Test
    public void shouldSkipGlobs() throws SchemaException {
        List<String> includeConfiguration = List.of("*");
        List<String> skipConfiguration = List.of("t*1");

        List<Table> filteredTables = Table.filterDFS(BASIC_TABLES, includeConfiguration, skipConfiguration, false);

        assertThat(extractTableNames(filteredTables)).containsOnly("table2", "table3");
    }

    @Test
    public void shouldReturnTheParentAndAllDescendants() throws SchemaException {
        List<String> includeConfiguration = List.of("main_table");

        List<Table> filteredTables = Table.filterDFS(NESTED_TABLE, includeConfiguration, EMPTY_CONFIGURATION, false);

        assertThat(extractTableNames(filteredTables)).containsOnly("main_table", "sub_sub_table", "sub_table");
    }

    @Test
    public void shouldThrowExceptionIfNoIncludeMatches() {
        String tableMatch = "bad_match";
        List<String> includeConfiguration = List.of(tableMatch);

        SchemaException schemaException = assertThrows(SchemaException.class, () -> Table.filterDFS(NESTED_TABLE, includeConfiguration, EMPTY_CONFIGURATION, false));
        assertEquals("table configuration includes a pattern \"" + tableMatch + "\" with no matches", schemaException.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfNoExcludeMatches() {
        String tableMatch = "bad_match";
        List<String> includeConfiguration = List.of("*");
        List<String> skipConfiguration = List.of(tableMatch);

        SchemaException schemaException = assertThrows(SchemaException.class, () -> Table.filterDFS(NESTED_TABLE, includeConfiguration, skipConfiguration, false));
        assertEquals("skip configuration includes a pattern \"" + tableMatch + "\" with no matches", schemaException.getMessage());
    }

    @Test
    public void shouldSkipChildTableButReturnSiblings() throws SchemaException {
        List<Table> tables = List.of(
                Table.builder().name("main_table").relations(
                        List.of(
                                Table.builder().name("sub_table_1").parent(Table.builder().name("main_table").build()).build(),
                                Table.builder().name("sub_table_2").parent(Table.builder().name("main_table").build()).build()
                        )
                ).build()
        );

        List<String> includeTables = List.of("main_table");
        List<String> skipTables = List.of("sub_table_2");

        List<Table> filteredTables = Table.filterDFS(tables, includeTables, skipTables, false);

        assertThat(extractTableNames(filteredTables)).containsOnly("main_table", "sub_table_1");
    }

    @Test
    public void shouldSkipChildTablesIfSkipDependentTrue() throws SchemaException {
        List<Table> tables = List.of(
                Table.builder().name("main_table").relations(
                        List.of(
                                Table.builder().name("sub_table_1").parent(Table.builder().name("main_table").build()).build(),
                                Table.builder().name("sub_table_2").parent(Table.builder().name("main_table").build()).build()
                        )
                ).build()
        );

        List<String> includeTables = List.of("main_table");
        List<String> skipTables = List.of("sub_table_2");

        List<Table> filteredTables = Table.filterDFS(tables, includeTables, skipTables, true);

        assertThat(extractTableNames(filteredTables)).containsOnly("main_table");
    }

    @Test
    public void shouldSkipChildTablesIfSkipDependentTablesIsTrueButNotIfExplicitlyIncluded() throws SchemaException {
        List<Table> tables = List.of(
                Table.builder().name("main_table_1").relations(
                        List.of(
                                Table.builder().name("sub_table_1").parent(Table.builder().name("main_table_1").build()).build()
                        )
                ).build(),
                Table.builder().name("main_table_2").relations(
                        List.of(
                                Table.builder().name("sub_table_2").parent(Table.builder().name("main_table_2").build()).build(),
                                Table.builder().name("sub_table_3").parent(Table.builder().name("main_table_2").build()).build()
                        )
                ).build()
        );

        List<String> includeTables = List.of("main_table_1", "sub_table_2");

        List<Table> filteredTables = Table.filterDFS(tables, includeTables, EMPTY_CONFIGURATION, true);

        assertThat(extractTableNames(filteredTables)).containsOnly("main_table_1", "main_table_2", "sub_table_2");
    }

    private List<String> extractTableNames(List<Table> filteredTables) {
        return Table.flattenTables(filteredTables).stream().map(Table::getName).toList();
    }
}
