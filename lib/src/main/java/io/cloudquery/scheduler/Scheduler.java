package io.cloudquery.scheduler;

import com.google.protobuf.ByteString;
import io.cloudquery.helper.ArrowHelper;
import io.cloudquery.plugin.v3.Sync;
import io.cloudquery.schema.ClientMeta;
import io.cloudquery.schema.Resource;
import io.cloudquery.schema.Table;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

  private void resolveTables(List<Table> tables, Resource parent, int concurrency)
      throws InterruptedException {
    if (tables == null || tables.isEmpty()) {
      return;
    }
    ExecutorService executor = Executors.newFixedThreadPool(Math.min(tables.size(), concurrency));
    for (Table table : tables) {
      final int nextLevelConcurrency = Math.max(1, concurrency / 2);
      executor.submit(
          new Runnable() {
            @Override
            public void run() {
              try {
                String tableMessage =
                    parent != null
                        ? "table " + table.getName() + " of parent" + parent.getTable().getName()
                        : "table " + table.getName();

                logger.info("resolving {}", tableMessage);
                if (!table.getResolver().isPresent()) {
                  logger.error("no resolver for {}", tableMessage);
                  return;
                }

                SchedulerTableOutputStream schedulerTableOutputStream =
                    new SchedulerTableOutputStream(table, parent, client, logger);
                table.getResolver().get().resolve(client, parent, schedulerTableOutputStream);

                for (Resource resource : schedulerTableOutputStream.getResources()) {
                  ByteString record = resource.encode();
                  Sync.MessageInsert insert =
                      Sync.MessageInsert.newBuilder().setRecord(record).build();
                  Sync.Response response = Sync.Response.newBuilder().setInsert(insert).build();
                  syncStream.onNext(response);
                  resolveTables(table.getRelations(), resource, nextLevelConcurrency);
                }

                logger.info("resolved {}", tableMessage);
              } catch (Exception e) {
                logger.error("Failed to resolve table: {}", table.getName(), e);
                syncStream.onError(e);
                return;
              }
            }
          });
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
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

    try {
      resolveTables(this.tables, null, this.concurrency);
    } catch (InterruptedException e) {
      logger.error("Failed to resolve tables", e);
      syncStream.onError(e);
      return;
    }

    syncStream.onCompleted();
  }
}
