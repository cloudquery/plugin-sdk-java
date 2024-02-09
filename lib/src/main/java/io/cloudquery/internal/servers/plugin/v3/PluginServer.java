package io.cloudquery.internal.servers.plugin.v3;

import com.google.protobuf.ByteString;
import io.cloudquery.helper.ArrowHelper;
import io.cloudquery.messages.WriteDeleteStale;
import io.cloudquery.messages.WriteInsert;
import io.cloudquery.messages.WriteMessage;
import io.cloudquery.messages.WriteMigrateTable;
import io.cloudquery.plugin.BackendOptions;
import io.cloudquery.plugin.NewClientOptions;
import io.cloudquery.plugin.Plugin;
import io.cloudquery.plugin.v3.PluginGrpc.PluginImplBase;
import io.cloudquery.plugin.v3.Write;
import io.cloudquery.plugin.v3.Write.MessageMigrateTable;
import io.cloudquery.scalar.ValidationException;
import io.cloudquery.schema.Table;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PluginServer extends PluginImplBase {
  private final Plugin plugin;

  public PluginServer(Plugin plugin) {
    this.plugin = plugin;
  }

  @Override
  public void getName(
      io.cloudquery.plugin.v3.GetName.Request request,
      StreamObserver<io.cloudquery.plugin.v3.GetName.Response> responseObserver) {
    responseObserver.onNext(
        io.cloudquery.plugin.v3.GetName.Response.newBuilder().setName(plugin.getName()).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getVersion(
      io.cloudquery.plugin.v3.GetVersion.Request request,
      StreamObserver<io.cloudquery.plugin.v3.GetVersion.Response> responseObserver) {
    responseObserver.onNext(
        io.cloudquery.plugin.v3.GetVersion.Response.newBuilder()
            .setVersion(plugin.getVersion())
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void init(
      io.cloudquery.plugin.v3.Init.Request request,
      StreamObserver<io.cloudquery.plugin.v3.Init.Response> responseObserver) {
    try {
      plugin.init(
          request.getSpec().toStringUtf8(),
          NewClientOptions.builder().noConnection(request.getNoConnection()).build());
      responseObserver.onNext(io.cloudquery.plugin.v3.Init.Response.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      plugin.getLogger().error("Error initializing plugin", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getTables(
      io.cloudquery.plugin.v3.GetTables.Request request,
      StreamObserver<io.cloudquery.plugin.v3.GetTables.Response> responseObserver) {
    try {
      List<Table> tables =
          plugin.tables(
              request.getTablesList(),
              request.getSkipTablesList(),
              request.getSkipDependentTables());
      List<ByteString> byteStrings = new ArrayList<>();
      for (Table table : Table.flattenTables(tables)) {
        byteStrings.add(ArrowHelper.encode(table));
      }
      responseObserver.onNext(
          io.cloudquery.plugin.v3.GetTables.Response.newBuilder()
              .addAllTables(byteStrings)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      plugin.getLogger().error("Error getting tables", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void sync(
      io.cloudquery.plugin.v3.Sync.Request request,
      StreamObserver<io.cloudquery.plugin.v3.Sync.Response> responseObserver) {
    try {
      plugin.sync(
          request.getTablesList(),
          request.getSkipTablesList(),
          request.getSkipDependentTables(),
          request.getDeterministicCqId(),
          new BackendOptions(
              request.getBackend().getTableName(), request.getBackend().getConnection()),
          responseObserver);
    } catch (Exception e) {
      plugin.getLogger().error("Error syncing tables", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void read(
      io.cloudquery.plugin.v3.Read.Request request,
      StreamObserver<io.cloudquery.plugin.v3.Read.Response> responseObserver) {
    plugin.read();
    responseObserver.onNext(io.cloudquery.plugin.v3.Read.Response.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public StreamObserver<Write.Request> write(StreamObserver<Write.Response> responseObserver) {
    return new StreamObserver<>() {
      @Override
      public void onNext(Write.Request request) {
        Write.Request.MessageCase messageCase = request.getMessageCase();
        try {
          if (messageCase == Write.Request.MessageCase.MIGRATE_TABLE) {
            plugin.write(processMigrateTableRequest(request));
          } else if (messageCase == Write.Request.MessageCase.INSERT) {
            plugin.write(processInsertRequest(request));
          } else if (messageCase == Write.Request.MessageCase.DELETE) {
            plugin.write(processDeleteStaleRequest(request));
          } else {
            throw new IllegalArgumentException("Unknown message type: " + messageCase);
          }
        } catch (IOException | ValidationException ex) {
          onError(ex);
        }
      }

      @Override
      public void onError(Throwable t) {
        responseObserver.onError(t);
      }

      @Override
      public void onCompleted() {
        responseObserver.onNext(Write.Response.newBuilder().build());
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public void close(
      io.cloudquery.plugin.v3.Close.Request request,
      StreamObserver<io.cloudquery.plugin.v3.Close.Response> responseObserver) {
    plugin.close();
    responseObserver.onNext(io.cloudquery.plugin.v3.Close.Response.newBuilder().build());
    responseObserver.onCompleted();
  }

  private WriteMigrateTable processMigrateTableRequest(Write.Request request) throws IOException {
    MessageMigrateTable migrateTable = request.getMigrateTable();
    ByteString byteString = migrateTable.getTable();
    boolean migrateForce = request.getMigrateTable().getMigrateForce();
    return new WriteMigrateTable(ArrowHelper.decode(byteString), migrateForce);
  }

  private WriteMessage processInsertRequest(Write.Request request)
      throws IOException, ValidationException {
    Write.MessageInsert insert = request.getInsert();
    ByteString record = insert.getRecord();
    return new WriteInsert(ArrowHelper.decodeResource(record));
  }

  private WriteMessage processDeleteStaleRequest(Write.Request request)
      throws IOException, ValidationException {
    Write.MessageDeleteStale messageDeleteStale = request.getDelete();
    return new WriteDeleteStale(
        messageDeleteStale.getTableName(),
        messageDeleteStale.getSourceName(),
        new Date(messageDeleteStale.getSyncTime().getSeconds() * 1000));
  }

  @Override
  public void getSpecSchema(
      io.cloudquery.plugin.v3.GetSpecSchema.Request request,
      StreamObserver<io.cloudquery.plugin.v3.GetSpecSchema.Response> responseObserver) {
    io.cloudquery.plugin.v3.GetSpecSchema.Response.Builder builder =
        io.cloudquery.plugin.v3.GetSpecSchema.Response.newBuilder();
    String schema = this.plugin.getJson_schema();
    if (schema != null && !schema.isBlank()) {
      builder.setJsonSchema(schema);
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }
}
