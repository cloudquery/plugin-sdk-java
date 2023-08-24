package io.cloudquery.memdb;

import io.cloudquery.messages.WriteDeleteStale;
import io.cloudquery.messages.WriteInsert;
import io.cloudquery.messages.WriteMessage;
import io.cloudquery.messages.WriteMigrateTable;
import io.cloudquery.scalar.Timestamp;
import io.cloudquery.schema.ClientMeta;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Resource;
import io.cloudquery.schema.Table;
import io.cloudquery.schema.TableColumnChange;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemDBClient implements ClientMeta {
  private static final String id = "memdb";

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Map<String, Table> tables = new HashMap<>();
  private final Map<String, List<Resource>> memDB = new HashMap<>();

  public MemDBClient() {}

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void write(WriteMessage message) {
    lock.writeLock().lock();
    try {
      if (message instanceof WriteMigrateTable migrateTable) {
        migrate(migrateTable);
      }
      if (message instanceof WriteInsert insert) {
        insert(insert);
      }
      if (message instanceof WriteDeleteStale deleteStale) {
        deleteStale(deleteStale);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void insert(WriteInsert insert) {
    String tableName = insert.getResource().getTable().getName();
    Table table = tables.get(tableName);
    overwrite(table, insert.getResource());
  }

  private void overwrite(Table table, Resource resource) {
    String tableName = table.getName();
    List<Integer> pkIndexes = table.primaryKeyIndexes();
    if (pkIndexes.isEmpty()) {
      memDB.get(tableName).add(resource);
      return;
    }

    for (int i = 0; i < memDB.get(tableName).size(); i++) {
      boolean found = true;
      for (int pkIndex : pkIndexes) {
        String s1 = resource.getTable().getColumns().get(pkIndex).getName();
        String s2 = memDB.get(tableName).get(i).getTable().getColumns().get(pkIndex).getName();
        if (!s1.equals(s2)) {
          found = false;
        }
      }
      if (found) {
        memDB.get(tableName).remove(i);
        memDB.get(tableName).add(resource);
        return;
      }
    }
    memDB.get(tableName).add(resource);
  }

  private void deleteStale(WriteDeleteStale deleteStale) {
    String tableName = deleteStale.getTableName();

    List<Resource> filteredList = new ArrayList<>();

    for (int i = 0; i < memDB.get(tableName).size(); i++) {
      Resource row = memDB.get(tableName).get(i);
      Optional<Column> sourceColumn = row.getTable().getColumn(Column.CQ_SOURCE_NAME);
      if (sourceColumn.isEmpty()) {
        continue;
      }
      Optional<Column> syncColumn = row.getTable().getColumn(Column.CQ_SYNC_TIME);
      if (syncColumn.isEmpty()) {
        continue;
      }

      String sourceName = "";
      if (row.get(Column.CQ_SOURCE_NAME) != null) {
        sourceName = row.get(Column.CQ_SOURCE_NAME).toString();
      }

      if (Objects.equals(sourceName, deleteStale.getSourceName())) {
        Date rowSyncTime = new Date(0);
        if (row.get(Column.CQ_SYNC_TIME) != null) {
          rowSyncTime = new Date(((Timestamp) row.get(Column.CQ_SYNC_TIME)).get());
        }
        if (!rowSyncTime.before(deleteStale.getTimestamp())) {
          filteredList.add(row);
        }
      }
    }
    memDB.put(tableName, filteredList);
  }

  public void close() {
    // do nothing
  }

  private void migrate(WriteMigrateTable migrateTable) {
    Table table = migrateTable.getTable();
    String tableName = table.getName();
    if (!memDB.containsKey(tableName)) {
      memDB.put(tableName, new ArrayList<>());
      tables.put(tableName, table);
      return;
    }

    List<TableColumnChange> changes = table.getChanges(tables.get(tableName));
    if (changes.isEmpty()) {
      return;
    }
    memDB.put(tableName, new ArrayList<>());
    tables.put(tableName, table);
  }
}
