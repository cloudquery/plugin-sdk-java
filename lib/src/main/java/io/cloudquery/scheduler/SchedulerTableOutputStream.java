package io.cloudquery.scheduler;

import io.cloudquery.plugin.TableOutputStream;
import io.cloudquery.schema.ClientMeta;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Resource;
import io.cloudquery.schema.Table;
import io.cloudquery.transformers.TransformerException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import org.apache.logging.log4j.Logger;

public class SchedulerTableOutputStream implements TableOutputStream {
  private static final int RESOURCE_RESOLVE_CONCURRENCY = 100;
  private static final int RESOURCE_RESOLVE_TIMEOUT_MINUTES = 10;
  @NonNull private final Table table;
  private final Resource parent;
  @NonNull private final ClientMeta client;
  @NonNull private final Logger logger;

  private List<Resource> resources = new ArrayList<Resource>();

  private ExecutorService executor;

  public SchedulerTableOutputStream(
      @NonNull Table table, Resource parent, @NonNull ClientMeta client, @NonNull Logger logger) {
    this.table = table;
    this.parent = parent;
    this.client = client;
    this.logger = logger;
    this.executor = Executors.newFixedThreadPool(RESOURCE_RESOLVE_CONCURRENCY);
  }

  @Override
  public void write(Object data) {
    Resource resource = Resource.builder().table(table).parent(parent).item(data).build();
    for (Column column : table.getColumns()) {
      executor.submit(
          new Runnable() {
            @Override
            public void run() {
              try {
                logger.debug("resolving column: {}", column.getName());
                if (column.getResolver() == null) {
                  logger.error("no resolver for column: {}", column.getName());
                  return;
                }
                column.getResolver().resolve(client, resource, column);
                logger.debug("resolved column: {}", column.getName());
                return;
              } catch (TransformerException e) {
                logger.error("Failed to resolve column: {}", column.getName(), e);
                return;
              }
            }
          });
    }
    resources.add(resource);
  }

  public List<Resource> getResources() throws InterruptedException {
    // TODO: Optimize this to not wait for all futures to complete and return resolved resources
    // first
    executor.shutdown();
    executor.awaitTermination(RESOURCE_RESOLVE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
    return this.resources;
  }
}
