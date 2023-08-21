package io.cloudquery.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudquery.types.JSONType.JSONVector;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JSONTypeTest {
  private static final String FIELD_NAME = "json";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Data
  @AllArgsConstructor
  public static class Person {
    private String name;
    private Integer age;
    private List<String> hobbies;
  }

  private File file;
  private List<String> jsonData;

  @BeforeAll
  public static void setUpTest() {
    ExtensionTypeRegistry.register(new JSONType());
  }

  @AfterAll
  public static void tearDown() {
    ExtensionTypeRegistry.unregister(new JSONType());
  }

  @BeforeEach
  void setUp() throws IOException {
    file = File.createTempFile("json_test", ".arrow");
    jsonData =
        List.of(
            toJSON(new Person("John", 30, List.of("hiking", "swimming"))),
            toJSON(new Person("Jane", 25, List.of("reading", "cooking"))));
  }

  @Test
  public void shouldSetJSONOnJSONVector() throws IOException {

    try (BufferAllocator allocator = new RootAllocator()) {
      ArrowType.ExtensionType jsonType = ExtensionTypeRegistry.lookup("json");
      try (JSONVector vector = (JSONVector) jsonType.getNewVector("vector", null, allocator)) {
        vector.set(0, jsonData.get(0));
        vector.setNull(1);
        vector.set(2, jsonData.get(1));
        vector.setNull(3);
        vector.setValueCount(4);

        // Assert that the values were set correctly
        assertEquals(jsonData.get(0), vector.get(0), "JSON should match");
        assertTrue(vector.isNull(1), "Should be null");
        assertEquals(jsonData.get(1), vector.get(2), "JSON should match");
        assertTrue(vector.isNull(3), "Should be null");

        // Assert that the value count and null count are correct
        assertEquals(4, vector.getValueCount(), "Value count should match");
        assertEquals(2, vector.getNullCount(), "Null count should match");
      }
    }
  }

  @Test
  public void roundTripJSON() throws IOException {
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

      JSONVector fieldVector = (JSONVector) reader.getVectorSchemaRoot().getVector(FIELD_NAME);
      assertEquals(jsonData.size(), fieldVector.getValueCount(), "Value count should match");
      for (int i = 0; i < jsonData.size(); i++) {
        assertEquals(jsonData.get(i), fieldVector.get(i), "JSON should match");
      }
    }
  }

  private static VectorSchemaRoot createVectorSchemaRoot(BufferAllocator allocator) {
    return VectorSchemaRoot.create(
        new Schema(Collections.singletonList(Field.nullable(FIELD_NAME, new JSONType()))),
        allocator);
  }

  private void generateDataAndWriteToFile(VectorSchemaRoot root) throws IOException {
    // Get the vector representing the column
    JSONVector vector = (JSONVector) root.getVector(FIELD_NAME);

    // Generate some JSON data
    vector.setValueCount(jsonData.size());
    for (int i = 0; i < jsonData.size(); i++) {
      vector.set(i, jsonData.get(i));
    }
    root.setRowCount(jsonData.size());

    // Write the data to a file
    try (WritableByteChannel channel =
            FileChannel.open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE);
        ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }

  private static String toJSON(Object object) throws IOException {
    try (OutputStream outputStream = new ByteArrayOutputStream()) {
      OBJECT_MAPPER.writeValue(outputStream, object);
      return outputStream.toString();
    }
  }
}
