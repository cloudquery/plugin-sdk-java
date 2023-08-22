package io.cloudquery.memdb;

import io.cloudquery.messages.WriteMessage;
import io.cloudquery.plugin.BackendOptions;
import io.cloudquery.plugin.ClientNotInitializedException;
import io.cloudquery.plugin.NewClientOptions;
import io.cloudquery.plugin.Plugin;
import io.cloudquery.plugin.TableOutputStream;
import io.cloudquery.scheduler.Scheduler;
import io.cloudquery.schema.ClientMeta;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Resource;
import io.cloudquery.schema.SchemaException;
import io.cloudquery.schema.Table;
import io.cloudquery.schema.TableResolver;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;

public class MemDB extends Plugin {
  private List<Table> allTables =
      List.of(
          Table.builder()
              .name("table1")
              .resolver(
                  new TableResolver() {
                    @Override
                    public void resolve(
                        ClientMeta clientMeta, Resource parent, TableOutputStream stream) {
                      stream.write(Table1Data.builder().name("name1").build());
                      stream.write(Table1Data.builder().name("name2").build());
                    }
                  })
              .columns(List.of(Column.builder().name("name").type(new Utf8()).build()))
              .build(),
          Table.builder()
              .name("table2")
              .resolver(
                  new TableResolver() {
                    @Override
                    public void resolve(
                        ClientMeta clientMeta, Resource parent, TableOutputStream stream) {
                      stream.write(Table2Data.builder().id("id1").build());
                      stream.write(Table2Data.builder().id("id2").build());
                    }
                  })
              .columns(List.of(Column.builder().name("id").type(new Utf8()).build()))
              .build());

  private Spec spec;

  public MemDB() {
    super("memdb", "0.0.1");
  }

  @Override
  public List<Table> tables(
      List<String> includeList, List<String> skipList, boolean skipDependentTables)
      throws SchemaException, ClientNotInitializedException {
    if (this.client == null) {
      throw new ClientNotInitializedException();
    }
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
      throws SchemaException, ClientNotInitializedException {
    if (this.client == null) {
      throw new ClientNotInitializedException();
    }

    List<Table> filtered = Table.filterDFS(allTables, includeList, skipList, skipDependentTables);
    Scheduler.builder()
        .client(client)
        .tables(filtered)
        .syncStream(syncStream)
        .deterministicCqId(deterministicCqId)
        .logger(getLogger())
        .concurrency(this.spec.getConcurrency())
        .build()
        .sync();
  }

  @Override
  public void read() {
    throw new UnsupportedOperationException("Unimplemented method 'Read'");
  }

  @Override
  public void write(WriteMessage message) {
    client.write(message);
  }

  @Override
  public void close() {
    if (this.client != null) {
      ((MemDBClient) this.client).close();
    }
  }

  @Override
  public ClientMeta newClient(String spec, NewClientOptions options) throws Exception {
    if (options.isNoConnection()) {
      return null;
    }
    this.spec = Spec.fromJSON(spec);
    return new MemDBClient();
  }
}
