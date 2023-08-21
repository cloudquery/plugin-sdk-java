package io.cloudquery.memdb;

import io.cloudquery.plugin.BackendOptions;
import io.cloudquery.plugin.Plugin;
import io.cloudquery.scheduler.Scheduler;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.SchemaException;
import io.cloudquery.schema.Table;
import io.grpc.stub.StreamObserver;
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
  public List<Table> tables(
      List<String> includeList, List<String> skipList, boolean skipDependentTables)
      throws SchemaException {
    return Table.filterDFS(allTables, includeList, skipList, skipDependentTables);
  }

  @Override
  public void sync(
      List<String> includeList,
      List<String> skipList,
      boolean skipDependentTables,
      boolean deterministicCqId,
      BackendOptions backendOptions,
      StreamObserver<io.cloudquery.plugin.v3.Sync.Response> syncStream)
      throws SchemaException {
    List<Table> filtered = Table.filterDFS(allTables, includeList, skipList, skipDependentTables);
    Scheduler.builder()
        .tables(filtered)
        .syncStream(syncStream)
        .deterministicCqId(deterministicCqId)
        .logger(getLogger())
        .build()
        .sync();
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
