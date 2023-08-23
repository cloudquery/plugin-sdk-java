package io.cloudquery.helper;

import static java.util.Arrays.asList;

import com.google.protobuf.ByteString;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Table;
import io.cloudquery.schema.Table.TableBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowHelper {
  public static final String CQ_TABLE_NAME = "cq:table_name";
  public static final String CQ_TABLE_TITLE = "cq:table_title";
  public static final String CQ_TABLE_DESCRIPTION = "cq:table_description";
  public static final String CQ_TABLE_DEPENDS_ON = "cq:table_depends_on";

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

  public static Table decode(ByteString byteString) throws IOException {
    try (BufferAllocator bufferAllocator = new RootAllocator()) {
      try (ArrowReader reader = new ArrowStreamReader(byteString.newInput(), bufferAllocator)) {
        VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
        return fromArrowSchema(vectorSchemaRoot.getSchema());
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
    metadata.put(CQ_TABLE_NAME, table.getName());
    if (table.getTitle() != null) {
      metadata.put(CQ_TABLE_TITLE, table.getTitle());
    }
    if (table.getDescription() != null) {
      metadata.put(CQ_TABLE_DESCRIPTION, table.getDescription());
    }
    if (table.getParent() != null) {
      metadata.put(CQ_TABLE_DEPENDS_ON, table.getParent().getName());
    }
    return new Schema(asList(fields), metadata);
  }

  public static Table fromArrowSchema(Schema schema) {
    List<Column> columns = new ArrayList<>();
    for (Field field : schema.getFields()) {
      columns.add(Column.builder().name(field.getName()).type(field.getType()).build());
    }

    Map<String, String> metaData = schema.getCustomMetadata();
    String name = metaData.get(CQ_TABLE_NAME);
    String title = metaData.get(CQ_TABLE_TITLE);
    String description = metaData.get(CQ_TABLE_DESCRIPTION);
    String parent = metaData.get(CQ_TABLE_DEPENDS_ON);

    TableBuilder tableBuilder = Table.builder().name(name).columns(columns);
    if (title != null) {
      tableBuilder.title(title);
    }
    if (description != null) {
      tableBuilder.description(description);
    }
    if (parent != null) {
      tableBuilder.parent(Table.builder().name(parent).build());
    }

    return tableBuilder.build();
  }
}
