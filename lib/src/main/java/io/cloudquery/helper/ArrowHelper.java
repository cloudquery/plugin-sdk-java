package io.cloudquery.helper;

import static java.util.Arrays.asList;

import com.google.protobuf.ByteString;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Table;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowHelper {
  public static ByteString encode(Table table) throws IOException {
    try (BufferAllocator bufferAllocator = new RootAllocator()) {
      Schema schema = toArrowSchema(table);
      VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, bufferAllocator);
      try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        try (ArrowStreamWriter writer =
            new ArrowStreamWriter(schemaRoot, null, Channels.newChannel(out))) {
          writer.start();
          writer.end();
          return ByteString.copyFrom(out.toByteArray());
        }
      }
    }
  }

  public static Schema toArrowSchema(Table table) {
    List<Column> columns = table.getColumns();
    Field[] fields = new Field[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      Field field = Field.nullable(column.getName(), column.getType());
      fields[i] = field;
    }
    Map<String, String> metadata = new HashMap<>();
    metadata.put("cq:table_name", table.getName());
    if (table.getTitle() != null) {
      metadata.put("cq:table_title", table.getTitle());
    }
    if (table.getDescription() != null) {
      metadata.put("cq:table_description", table.getDescription());
    }
    if (table.getParent() != null) {
      metadata.put("cq:table_depends_on", table.getParent().getName());
    }
    return new Schema(asList(fields), metadata);
  }
}
