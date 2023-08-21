package io.cloudquery.scheduler;

import io.cloudquery.plugin.v3.Sync;
import io.cloudquery.schema.Table;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import org.apache.logging.log4j.Logger;

@Builder
public class Scheduler {
  @NonNull private final List<Table> tables;
  @NonNull private final StreamObserver<io.cloudquery.plugin.v3.Sync.Response> syncStream;
  @NonNull private final Logger logger;

  private boolean deterministicCqId;

  public void sync() {
    for (Table table : tables) {
      try {
        logger.info("sending migrate message for table: {}", table.getName());
        Sync.MessageMigrateTable migrateTable =
            Sync.MessageMigrateTable.newBuilder().setTable(table.encode()).build();
        Sync.Response response = Sync.Response.newBuilder().setMigrateTable(migrateTable).build();
        syncStream.onNext(response);
      } catch (IOException e) {
        syncStream.onError(e);
        return;
      }
    }
  }
}
