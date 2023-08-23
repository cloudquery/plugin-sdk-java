package io.cloudquery.memdb;

import io.cloudquery.messages.WriteMessage;
import io.cloudquery.messages.WriteMigrateTable;
import io.cloudquery.schema.ClientMeta;
import io.cloudquery.schema.Table;
import io.cloudquery.schema.TableColumnChange;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.arrow.vector.VectorSchemaRoot;

public class MemDBClient implements ClientMeta {
  private static final String id = "memdb";

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Map<String, Table> tables = new HashMap<>();
  private Map<String, List<VectorSchemaRoot>> memDB = new HashMap<>();

  public MemDBClient() {}

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void write(WriteMessage message) {
    if (message instanceof WriteMigrateTable migrateTable) {
      migrate(migrateTable);
    }
  }

  public void close() {
    // do nothing
  }

  private void migrate(WriteMigrateTable migrateTable) {
    lock.writeLock().lock();
    try {
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
    } finally {
      lock.writeLock().unlock();
    }
  }
}
