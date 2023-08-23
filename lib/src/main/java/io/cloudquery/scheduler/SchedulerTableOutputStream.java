package io.cloudquery.scheduler;

import com.google.protobuf.ByteString;
import io.cloudquery.plugin.TableOutputStream;
import io.cloudquery.plugin.v3.Sync;
import io.cloudquery.schema.ClientMeta;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Resource;
import io.cloudquery.schema.Table;
import io.cloudquery.transformers.TransformerException;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.NonNull;
import org.apache.logging.log4j.Logger;

@Builder
public class SchedulerTableOutputStream implements TableOutputStream {
  @NonNull private final Table table;
  private final Resource parent;
  @NonNull private final ClientMeta client;
  @NonNull private final Logger logger;
  @NonNull private final StreamObserver<io.cloudquery.plugin.v3.Sync.Response> syncStream;

  @Override
  public void write(Object data) {
    Resource resource = Resource.builder().table(table).parent(parent).item(data).build();
    for (Column column : table.getColumns()) {
      try {
        logger.info("resolving column: {}", column.getName());
        if (column.getResolver() == null) {
          logger.error("no resolver for column: {}", column.getName());
          continue;
        }
        column.getResolver().resolve(client, resource, column);
        logger.info("resolved column: {}", column.getName());
      } catch (TransformerException e) {
        logger.error("Failed to resolve column: {}", column.getName(), e);
        return;
      }
    }

    try {
      ByteString record = resource.encode();
      Sync.MessageInsert insert = Sync.MessageInsert.newBuilder().setRecord(record).build();
      Sync.Response response = Sync.Response.newBuilder().setInsert(insert).build();
      syncStream.onNext(response);
    } catch (Exception e) {
      logger.error("Failed to encode resource: {}", resource, e);
      return;
    }
  }
}
