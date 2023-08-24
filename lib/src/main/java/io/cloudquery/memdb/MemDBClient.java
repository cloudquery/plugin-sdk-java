package io.cloudquery.memdb;

import io.cloudquery.messages.WriteInsert;
import io.cloudquery.messages.WriteMessage;
import io.cloudquery.messages.WriteMigrateTable;
import io.cloudquery.schema.ClientMeta;
import io.cloudquery.schema.Resource;
import io.cloudquery.schema.Table;
import io.cloudquery.schema.TableColumnChange;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemDBClient implements ClientMeta {
  private static final String id = "memdb";

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Map<String, Table> tables = new HashMap<>();
  private Map<String, List<Resource>> memDB = new HashMap<>();

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
