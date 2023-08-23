package io.cloudquery.scheduler;

import io.cloudquery.helper.ArrowHelper;
import io.cloudquery.plugin.v3.Sync;
import io.cloudquery.schema.ClientMeta;
import io.cloudquery.schema.Table;
import io.grpc.stub.StreamObserver;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import org.apache.logging.log4j.Logger;

@Builder
public class Scheduler {
  @NonNull private final List<Table> tables;
  @NonNull private final StreamObserver<io.cloudquery.plugin.v3.Sync.Response> syncStream;
  @NonNull private final Logger logger;
  @NonNull private final ClientMeta client;

  private int concurrency;
  private boolean deterministicCqId;

  private void resolveTables(List<Table> tables, int concurrency) {
    for (Table table : tables) {
      try {
        logger.info("resolving table: {}", table.getName());
        if (!table.getResolver().isPresent()) {
          logger.error("no resolver for table: {}", table.getName());
          continue;
        }
        SchedulerTableOutputStream schedulerTableOutputStream =
            SchedulerTableOutputStream.builder()
                .table(table)
                .client(client)
                .logger(logger)
                .syncStream(syncStream)
                .build();
        table.getResolver().get().resolve(client, null, schedulerTableOutputStream);
        resolveTables(table.getRelations(), concurrency / 2);
        logger.info("resolved table: {}", table.getName());
      } catch (Exception e) {
        syncStream.onError(e);
        return;
      }
    }
  }

  public void sync() {
    for (Table table : Table.flattenTables(tables)) {
      try {
        logger.info("sending migrate message for table: {}", table.getName());
        Sync.MessageMigrateTable migrateTable =
            Sync.MessageMigrateTable.newBuilder().setTable(ArrowHelper.encode(table)).build();
        Sync.Response response = Sync.Response.newBuilder().setMigrateTable(migrateTable).build();
        syncStream.onNext(response);
      } catch (Exception e) {
        syncStream.onError(e);
        return;
      }
    }

    resolveTables(this.tables, this.concurrency);

    syncStream.onCompleted();
  }
}
