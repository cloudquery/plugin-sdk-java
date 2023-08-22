package io.cloudquery.internal.servers.plugin.v3;

import com.google.protobuf.ByteString;
import io.cloudquery.plugin.BackendOptions;
import io.cloudquery.plugin.NewClientOptions;
import io.cloudquery.plugin.Plugin;
import io.cloudquery.plugin.v3.PluginGrpc.PluginImplBase;
import io.cloudquery.plugin.v3.Write;
import io.cloudquery.schema.Table;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
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
        byteStrings.add(table.encode());
      }
      responseObserver.onNext(
          io.cloudquery.plugin.v3.GetTables.Response.newBuilder()
              .addAllTables(byteStrings)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
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
    plugin.write();
    return new StreamObserver<>() {
      @Override
      public void onNext(Write.Request request) {}

      @Override
      public void onError(Throwable t) {}

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
}
