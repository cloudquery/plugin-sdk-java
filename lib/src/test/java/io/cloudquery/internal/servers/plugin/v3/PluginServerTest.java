package io.cloudquery.internal.servers.plugin.v3;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import com.google.protobuf.ByteString;
import io.cloudquery.helper.ArrowHelper;
import io.cloudquery.messages.WriteDeleteStale;
import io.cloudquery.messages.WriteInsert;
import io.cloudquery.messages.WriteMigrateTable;
import io.cloudquery.plugin.Plugin;
import io.cloudquery.plugin.v3.GetSpecSchema;
import io.cloudquery.plugin.v3.PluginGrpc;
import io.cloudquery.plugin.v3.PluginGrpc.PluginStub;
import io.cloudquery.plugin.v3.Write;
import io.cloudquery.plugin.v3.Write.MessageInsert;
import io.cloudquery.scalar.ValidationException;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Resource;
import io.cloudquery.schema.Table;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import lombok.Getter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PluginServerTest {

  @Mock private Plugin plugin;

  @Rule public final GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule();

  private PluginStub pluginStub;

  @BeforeEach
  public void setUp() throws IOException {
    PluginServer pluginServer = new PluginServer(plugin);

    String generatedName = InProcessServerBuilder.generateName();
    Server server = InProcessServerBuilder.forName(generatedName).addService(pluginServer).build();
    server.start();

    InProcessChannelBuilder inProcessChannelBuilder =
        InProcessChannelBuilder.forName(generatedName).directExecutor();
    pluginStub = PluginGrpc.newStub(grpcCleanupRule.register(inProcessChannelBuilder.build()));
  }

  @Test
  public void shouldSendWriteMigrateTableMessage() throws Exception {
    NullResponseStream<Write.Response> responseObserver = new NullResponseStream<>();

    StreamObserver<Write.Request> writeService = pluginStub.write(responseObserver);
    writeService.onNext(generateMigrateTableMessage());
    writeService.onCompleted();
    responseObserver.await();

    verify(plugin).write(any(WriteMigrateTable.class));
  }

  @Test
  public void shouldSendWriteInsertMessage() throws Exception {
    NullResponseStream<Write.Response> responseObserver = new NullResponseStream<>();

    StreamObserver<Write.Request> writeService = pluginStub.write(responseObserver);
    writeService.onNext(generateInsertMessage());
    writeService.onCompleted();
    responseObserver.await();

    verify(plugin).write(any(WriteInsert.class));
  }

  @Test
  public void shouldSendWriteDeleteStaleMessage() throws Exception {
    NullResponseStream<Write.Response> responseObserver = new NullResponseStream<>();

    StreamObserver<Write.Request> writeService = pluginStub.write(responseObserver);
    writeService.onNext(generateDeleteStaleMessage());
    writeService.onCompleted();
    responseObserver.await();

    verify(plugin).write(any(WriteDeleteStale.class));
  }

  @Test
  public void shouldSendNullJSONSchema() throws Exception {
    NullResponseStream<GetSpecSchema.Response> responseObserver = new NullResponseStream<>();

    pluginStub.getSpecSchema(GetSpecSchema.Request.getDefaultInstance(), responseObserver);
    responseObserver.await();

    verify(plugin).getJsonSchema();
    assertFalse(responseObserver.getValue().hasJsonSchema());
  }

  @Test
  public void shouldSendNonNullJSONSchema() throws Exception {
    Mockito.doReturn("{}").when(plugin).getJsonSchema();

    NullResponseStream<GetSpecSchema.Response> responseObserver = new NullResponseStream<>();

    pluginStub.getSpecSchema(GetSpecSchema.Request.getDefaultInstance(), responseObserver);
    responseObserver.await();

    verify(plugin).getJsonSchema();
    assertTrue(responseObserver.getValue().hasJsonSchema());
    assertEquals("{}", responseObserver.getValue().getJsonSchema());
  }

  private static Write.Request generateMigrateTableMessage() throws IOException {
    Table table = Table.builder().name("test").build();
    return Write.Request.newBuilder()
        .setMigrateTable(
            Write.MessageMigrateTable.newBuilder().setTable(ArrowHelper.encode(table)).build())
        .build();
  }

  private Write.Request generateInsertMessage() throws IOException, ValidationException {
    Column stringColumn =
        Column.builder().name("test_column").type(ArrowType.Utf8.INSTANCE).build();
    Column booleanColumn =
        Column.builder().name("boolean_column").type(ArrowType.Bool.INSTANCE).build();
    Table table =
        Table.builder().name("test").columns(List.of(stringColumn, booleanColumn)).build();
    Resource resource = Resource.builder().table(table).build();
    resource.set("test_column", "test_data");
    resource.set("boolean_column", true);
    ByteString byteString = ArrowHelper.encode(resource);
    MessageInsert messageInsert = MessageInsert.newBuilder().setRecord(byteString).build();
    return Write.Request.newBuilder().setInsert(messageInsert).build();
  }

  private Write.Request generateDeleteStaleMessage() {
    Write.MessageDeleteStale messageDeleteStale = Write.MessageDeleteStale.newBuilder().build();
    return Write.Request.newBuilder().setDelete(messageDeleteStale).build();
  }

  private static class NullResponseStream<T> implements StreamObserver<T> {
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    @Getter private T value;
    @Getter private Throwable error;

    @Override
    public void onNext(T value) {
      this.value = value;
    }

    @Override
    public void onError(Throwable t) {
      this.error = t;
    }

    @Override
    public void onCompleted() {
      countDownLatch.countDown();
    }

    public void await() throws InterruptedException {
      countDownLatch.await();
    }
  }
}
