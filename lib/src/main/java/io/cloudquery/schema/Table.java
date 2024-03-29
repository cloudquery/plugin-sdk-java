package io.cloudquery.schema;

import static io.cloudquery.schema.TableColumnChangeType.ADD;
import static io.cloudquery.schema.TableColumnChangeType.REMOVE;
import static io.cloudquery.schema.TableColumnChangeType.UPDATE;
import static java.util.stream.Collectors.toList;

import io.cloudquery.glob.Glob;
import io.cloudquery.schema.Column.ColumnBuilder;
import io.cloudquery.transformers.TransformerException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Builder(toBuilder = true)
@Getter
public class Table {

  public interface Transform {
    void transformTable(Table table) throws TransformerException;
  }

  public static List<Table> flattenTables(List<Table> tables) {
    Map<String, Table> flattenMap = new LinkedHashMap<>();
    for (Table table : tables) {
      Table newTable = table.toBuilder().build();
      flattenMap.put(newTable.name, newTable);
      for (Table child : flattenTables(table.getRelations())) {
        flattenMap.put(child.name, child);
      }
    }
    return flattenMap.values().stream().toList();
  }

  public static List<Table> filterDFS(
      List<Table> tables,
      List<String> includeConfiguration,
      List<String> skipConfiguration,
      boolean skipDependentTables)
      throws SchemaException {
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
        throw new SchemaException(
            "table configuration includes a pattern \"" + includePattern + "\" with no matches");
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
        throw new SchemaException(
            "skip configuration includes a pattern \"" + excludePattern + "\" with no matches");
      }
    }

    Predicate<Table> include =
        table -> {
          for (String includePattern : includeConfiguration) {
            if (Glob.match(includePattern, table.getName())) {
              return true;
            }
          }
          return false;
        };

    Predicate<Table> exclude =
        table -> {
          for (String excludePattern : skipConfiguration) {
            if (Glob.match(excludePattern, table.getName())) {
              return true;
            }
          }
          return false;
        };

    return filterDFSFunc(tables, include, exclude, skipDependentTables);
  }

  private static List<Table> filterDFSFunc(
      List<Table> tables,
      Predicate<Table> include,
      Predicate<Table> exclude,
      boolean skipDependentTables) {
    List<Table> filteredTables = new ArrayList<>();
    for (Table table : tables) {
      Table filteredTable = table.toBuilder().parent(null).build();
      Optional<Table> optionalFilteredTable =
          filteredTable.filterDfs(false, include, exclude, skipDependentTables);
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

  @NonNull private String name;
  @Builder.Default private String constraintName = "";
  private TableResolver resolver;
  private String title;
  private String description;
  @Setter private Table parent;
  @Builder.Default private List<Column> columns = new ArrayList<>();

  @Builder.Default private List<Table> relations = new ArrayList<>();

  private Transform transform;

  public void transform() throws TransformerException {
    if (transform != null) {
      transform.transformTable(this);
    }
  }

  public void addCQIDs() {
    boolean havePrimaryKeys = !primaryKeys().isEmpty();
    ColumnBuilder cqIdColumnBuilder = Column.CQ_ID_COLUMN.toBuilder();
    if (!havePrimaryKeys) {
      cqIdColumnBuilder.primaryKey(true);
    }

    List<Column> newColumns = List.of(cqIdColumnBuilder.build(), Column.CQ_PARENT_ID_COLUMN);
    for (int i = 0; i < newColumns.size(); i++) {
      columns.add(i, newColumns.get(i));
    }

    for (Table relation : relations) {
      relation.addCQIDs();
    }
  }

  public int indexOfColumn(String columnName) {
    for (int index = 0; index < columns.size(); index++) {
      if (columns.get(index).getName().equals(columnName)) {
        return index;
      }
    }
    return -1;
  }

  public List<String> primaryKeys() {
    return columns.stream().filter(Column::isPrimaryKey).map(Column::getName).toList();
  }

  public List<Integer> primaryKeyIndexes() {
    return IntStream.range(0, columns.size())
        .filter(i -> columns.get(i).isPrimaryKey())
        .boxed()
        .collect(toList());
  }

  private Optional<Table> filterDfs(
      boolean parentMatched,
      Predicate<Table> include,
      Predicate<Table> exclude,
      boolean skipDependentTables) {
    if (exclude.test(this)) {
      return Optional.empty();
    }
    boolean matched = parentMatched && !skipDependentTables;
    if (include.test(this)) {
      matched = true;
    }
    List<Table> filteredRelations = new ArrayList<>();
    for (Table relation : relations) {
      Optional<Table> filteredChild =
          relation.filterDfs(matched, include, exclude, skipDependentTables);
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

  public Optional<Column> getColumn(String name) {
    for (Column column : columns) {
      if (column.getName().equals(name)) {
        return Optional.of(column);
      }
    }
    return Optional.empty();
  }

  public Optional<TableResolver> getResolver() {
    return Optional.ofNullable(resolver);
  }

  public List<TableColumnChange> getChanges(Table old) {
    List<TableColumnChange> changes = new ArrayList<>();
    for (Column currentColumn : columns) {
      Optional<Column> oldColumn = old.getColumn(currentColumn.getName());
      if (oldColumn.isEmpty()) {
        changes.add(new TableColumnChange(ADD, currentColumn.getName(), currentColumn, null));
        continue;
      }
      if (shouldUpdate(currentColumn, oldColumn.get())) {
        changes.add(
            new TableColumnChange(UPDATE, currentColumn.getName(), currentColumn, oldColumn.get()));
      }
    }
    for (Column column : old.columns) {
      Optional<Column> otherColumn = getColumn(column.getName());
      if (otherColumn.isEmpty()) {
        changes.add(new TableColumnChange(REMOVE, column.getName(), null, column));
      }
    }
    return changes;
  }

  private static boolean shouldUpdate(Column currentColumn, Column oldColumn) {
    if (!oldColumn.getType().equals(currentColumn.getType())) {
      return true;
    }
    if (oldColumn.isPrimaryKey() != currentColumn.isPrimaryKey()) {
      return true;
    }
    if (oldColumn.isNotNull() != currentColumn.isNotNull()) {
      return true;
    }
    return oldColumn.isUnique() != currentColumn.isUnique();
  }
}
