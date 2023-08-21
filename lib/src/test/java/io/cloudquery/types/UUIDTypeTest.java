package io.cloudquery.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cloudquery.types.UUIDType.UUIDVector;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UUIDTypeTest {
  private static final String FIELD_NAME = "uuid";
  private static final List<UUID> UUIDS =
      IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).toList();

  private File file;

  @BeforeAll
  public static void setUpTest() {
    ExtensionTypeRegistry.register(new UUIDType());
  }

  @AfterAll
  public static void tearDown() {
    ExtensionTypeRegistry.unregister(new UUIDType());
  }

  @BeforeEach
  void setUp() throws IOException {
    file = File.createTempFile("uuid_test", ".arrow");
  }

  @Test
  public void shouldSetUUIDsOnUUIDVector() {
    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();

    try (BufferAllocator allocator = new RootAllocator()) {
      ExtensionType uuidType = ExtensionTypeRegistry.lookup("uuid");
      try (UUIDVector vector = (UUIDVector) uuidType.getNewVector("vector", null, allocator)) {
        vector.setValueCount(4);
        vector.set(0, uuid1);
        vector.setNull(1);
        vector.set(2, uuid2);
        vector.setNull(3);

        // Assert that the values were set correctly
        assertEquals(uuid1, vector.get(0), "UUIDs should match");
        assertTrue(vector.isNull(1), "Should be null");
        assertEquals(uuid2, vector.get(2), "UUIDs should match");
        assertTrue(vector.isNull(3), "Should be null");

        // Assert that the value count and null count are correct
        assertEquals(4, vector.getValueCount(), "Value count should match");
        assertEquals(2, vector.getNullCount(), "Null count should match");
      }
    }
  }

  @Test
  public void roundTripUUID() throws IOException {
    // Generate some data and write it to a file
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = createVectorSchemaRoot(allocator)) {
      generateDataAndWriteToFile(root);
    }

    // Read the data back from the file and assert that it matches what we wrote
    try (BufferAllocator allocator = new RootAllocator();
        ArrowFileReader reader =
            new ArrowFileReader(
                Files.newByteChannel(Paths.get(file.getAbsolutePath())), allocator)) {

      reader.loadNextBatch();

      FieldVector fieldVector = reader.getVectorSchemaRoot().getVector(FIELD_NAME);
      assertEquals(UUIDS.size(), fieldVector.getValueCount(), "Value count should match");
      for (int i = 0; i < UUIDS.size(); i++) {
        assertEquals(UUIDS.get(i), fieldVector.getObject(i), "UUIDs should match");
      }
    }
  }

  private static VectorSchemaRoot createVectorSchemaRoot(BufferAllocator allocator) {
    return VectorSchemaRoot.create(
        new Schema(Collections.singletonList(Field.nullable(FIELD_NAME, new UUIDType()))),
        allocator);
  }

  private void generateDataAndWriteToFile(VectorSchemaRoot root) throws IOException {
    // Get the vector representing the column
    UUIDVector vector = (UUIDVector) root.getVector(FIELD_NAME);

    // Generate some UUIDs
    vector.setValueCount(UUIDS.size());
    for (int i = 0; i < UUIDS.size(); i++) {
      vector.set(i, UUIDS.get(i));
    }
    root.setRowCount(UUIDS.size());

    // Write the data to a file
    try (WritableByteChannel channel =
            FileChannel.open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE);
        ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }
}
