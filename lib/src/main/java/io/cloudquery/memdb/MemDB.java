package io.cloudquery.memdb;

import io.cloudquery.plugin.Plugin;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.SchemaException;
import io.cloudquery.schema.Table;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;

public class MemDB extends Plugin {
  private List<Table> allTables =
      List.of(
          Table.builder()
              .name("table1")
              .columns(List.of(Column.builder().name("name1").type(new Utf8()).build()))
              .build(),
          Table.builder()
              .name("table2")
              .columns(List.of(Column.builder().name("name1").type(new Utf8()).build()))
              .build());

  public MemDB() {
    super("memdb", "0.0.1");
  }

  @Override
  public void init() {
    // do nothing
  }

  @Override
  public List<Table> tables() throws SchemaException {
    return Table.filterDFS(allTables, List.of("*"), List.of(), false);
  }

  @Override
  public void sync() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'Sync'");
  }

  @Override
  public void read() {
    throw new UnsupportedOperationException("Unimplemented method 'Read'");
  }

  @Override
  public void write() {
    throw new UnsupportedOperationException("Unimplemented method 'Write'");
  }

  @Override
  public void close() {
    // do nothing
  }
}
