package io.cloudquery.schema;

import lombok.Builder;
import lombok.Getter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Builder.Default
    private List<Table> relations = Collections.emptyList();
}
