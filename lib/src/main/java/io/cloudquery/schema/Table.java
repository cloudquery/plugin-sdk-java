package io.cloudquery.schema;

import io.cloudquery.glob.Glob;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

@Builder(toBuilder = true)
@Getter
public class Table {
    public static List<Table> flattenTables(List<Table> tables) {
        Map<String, Table> flattenMap = new HashMap<>();
        for (Table table : tables) {
            Table newTable = table.toBuilder().relations(Collections.emptyList()).build();
            flattenMap.put(newTable.name, newTable);
            for (Table child : flattenTables(table.getRelations())) {
                flattenMap.put(child.name, child);
            }
        }
        return flattenMap.values().stream().toList();
    }

    public static List<Table> filterDFS(List<Table> tables, List<String> includeConfiguration, List<String> skipConfiguration, boolean skipDependentTables) throws SchemaException {
        List<Table> flattenedTables = flattenTables(tables);
        for (String includePattern : includeConfiguration) {
            boolean includeMatch = false;
            for (Table table : flattenedTables) {
                if (Glob.match(includePattern, table.getName())) {
                    includeMatch = true;
                    break;
                }
            }
            if (!includeMatch) {
                throw new SchemaException("table configuration includes a pattern \"" + includePattern + "\" with no matches");
            }
        }
        for (String excludePattern : skipConfiguration) {
            boolean excludeMatch = false;
            for (Table table : flattenedTables) {
                if (Glob.match(excludePattern, table.getName())) {
                    excludeMatch = true;
                    break;
                }
            }
            if (!excludeMatch) {
                throw new SchemaException("skip configuration includes a pattern \"" + excludePattern + "\" with no matches");
            }
        }

        Predicate<Table> include = table -> {
            for (String includePattern : includeConfiguration) {
                if (Glob.match(includePattern, table.getName())) {
                    return true;
                }
            }
            return false;
        };

        Predicate<Table> exclude = table -> {
            for (String excludePattern : skipConfiguration) {
                if (Glob.match(excludePattern, table.getName())) {
                    return true;
                }
            }
            return false;
        };

        return filterDFSFunc(tables, include, exclude, skipDependentTables);
    }

    private static List<Table> filterDFSFunc(List<Table> tables, Predicate<Table> include, Predicate<Table> exclude, boolean skipDependentTables) {
        List<Table> filteredTables = new ArrayList<>();
        for (Table table : tables) {
            Table filteredTable = table.toBuilder().parent(null).build();
            Optional<Table> optionalFilteredTable = filteredTable.filterDfs(false, include, exclude, skipDependentTables);
            optionalFilteredTable.ifPresent(filteredTables::add);
        }
        return filteredTables;
    }

    public static int maxDepth(List<Table> tables) {
        int depth = 0;
        if (tables.isEmpty()) {
            return 0;
        }
        for (Table table : tables) {
            int newDepth = 1 + maxDepth(table.getRelations());
            if (newDepth > depth) {
                depth = newDepth;
            }
        }
        return depth;
    }

    private String name;

    private Table parent;

    @Builder.Default
    private List<Table> relations = Collections.emptyList();

    private Optional<Table> filterDfs(boolean parentMatched, Predicate<Table> include, Predicate<Table> exclude, boolean skipDependentTables) {
        if (exclude.test(this)) {
            return Optional.empty();
        }
        boolean matched = parentMatched && !skipDependentTables;
        if (include.test(this)) {
            matched = true;
        }
        List<Table> filteredRelations = new ArrayList<>();
        for (Table relation : relations) {
            Optional<Table> filteredChild = relation.filterDfs(matched, include, exclude, skipDependentTables);
            if (filteredChild.isPresent()) {
                matched = true;
                filteredRelations.add(filteredChild.get());
            }
        }
        this.relations = filteredRelations;
        if (matched) {
            return Optional.of(this);
        }
        return Optional.empty();
    }

}
