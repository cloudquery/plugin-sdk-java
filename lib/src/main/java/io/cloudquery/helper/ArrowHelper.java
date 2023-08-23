package io.cloudquery.helper;

import static java.util.Arrays.asList;

import com.google.protobuf.ByteString;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Resource;
import io.cloudquery.schema.Table;
import io.cloudquery.schema.Table.TableBuilder;
import io.cloudquery.types.JSONType.JSONVector;
import io.cloudquery.types.UUIDType.UUIDVector;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

public class ArrowHelper {
  public static final String CQ_EXTENSION_INCREMENTAL = "cq:extension:incremental";
  public static final String CQ_EXTENSION_CONSTRAINT_NAME = "cq:extension:constraint_name";
  public static final String CQ_EXTENSION_PRIMARY_KEY = "cq:extension:primary_key";
  public static final String CQ_EXTENSION_UNIQUE = "cq:extension:unique";
  public static final String CQ_TABLE_NAME = "cq:table_name";
  public static final String CQ_TABLE_TITLE = "cq:table_title";
  public static final String CQ_TABLE_DESCRIPTION = "cq:table_description";
  public static final String CQ_TABLE_DEPENDS_ON = "cq:table_depends_on";

  private static void setVectorData(FieldVector vector, Object data) {
    vector.allocateNew();
    if (vector instanceof BigIntVector) {
      ((BigIntVector) vector).set(0, (long) data);
      return;
    }
    if (vector instanceof BitVector) {
      ((BitVector) vector).set(0, (int) data);
      return;
    }
    if (vector instanceof FixedSizeBinaryVector) {
      ((FixedSizeBinaryVector) vector).set(0, (byte[]) data);
      return;
    }
    if (vector instanceof Float4Vector) {
      ((Float4Vector) vector).set(0, (float) data);
      return;
    }
    if (vector instanceof Float8Vector) {
      ((Float8Vector) vector).set(0, (double) data);
      return;
    }
    if (vector instanceof IntVector) {
      ((IntVector) vector).set(0, (int) data);
      return;
    }
    if (vector instanceof LargeVarBinaryVector) {
      ((LargeVarBinaryVector) vector).set(0, (byte[]) data);
      return;
    }
    if (vector instanceof LargeVarCharVector) {
      ((LargeVarCharVector) vector).set(0, (Text) data);
      return;
    }
    if (vector instanceof SmallIntVector) {
      ((SmallIntVector) vector).set(0, (short) data);
      return;
    }
    if (vector instanceof TimeStampVector) {
      ((TimeStampVector) vector).set(0, (long) data);
      return;
    }
    if (vector instanceof TinyIntVector) {
      ((TinyIntVector) vector).set(0, (byte) data);
      return;
    }
    if (vector instanceof UInt1Vector) {
      ((UInt1Vector) vector).set(0, (byte) data);
      return;
    }
    if (vector instanceof UInt2Vector) {
      ((UInt2Vector) vector).set(0, (short) data);
      return;
    }
    if (vector instanceof UInt4Vector) {
      ((UInt4Vector) vector).set(0, (int) data);
      return;
    }
    if (vector instanceof UInt8Vector) {
      ((UInt8Vector) vector).set(0, (long) data);
      return;
    }
    if (vector instanceof VarBinaryVector) {
      ((VarBinaryVector) vector).set(0, (byte[]) data);
      return;
    }
    if (vector instanceof VarCharVector) {
      ((VarCharVector) vector).set(0, (Text) data);
      return;
    }
    if (vector instanceof UUIDVector) {
      ((UUIDVector) vector).set(0, (java.util.UUID) data);
      return;
    }
    if (vector instanceof JSONVector) {
      ((JSONVector) vector).setSafe(0, (byte[]) data);
      return;
    }

    throw new IllegalArgumentException("Unsupported vector type: " + vector.getClass());
  }

  public static ByteString encode(Table table) throws IOException {
    try (BufferAllocator bufferAllocator = new RootAllocator()) {
      Schema schema = toArrowSchema(table);
      try (VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, bufferAllocator)) {
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
      Map<String, String> metadata = new HashMap<>();
      metadata.put(CQ_EXTENSION_UNIQUE, column.isUnique() ? "true" : "false");
      metadata.put(CQ_EXTENSION_PRIMARY_KEY, column.isPrimaryKey() ? "true" : "false");
      metadata.put(CQ_EXTENSION_INCREMENTAL, column.isIncrementalKey() ? "true" : "false");
      Field field =
          new Field(
              column.getName(),
              new FieldType(!column.isNotNull(), column.getType(), null, metadata),
              null);
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
    metadata.put(CQ_EXTENSION_CONSTRAINT_NAME, table.getConstraintName());
    return new Schema(asList(fields), metadata);
  }

  public static Table fromArrowSchema(Schema schema) {
    List<Column> columns = new ArrayList<>();
    for (Field field : schema.getFields()) {
      boolean isUnique = field.getMetadata().get(CQ_EXTENSION_UNIQUE) == "true";
      boolean isPrimaryKey = field.getMetadata().get(CQ_EXTENSION_PRIMARY_KEY) == "true";
      boolean isIncrementalKey = field.getMetadata().get(CQ_EXTENSION_INCREMENTAL) == "true";

      columns.add(
          Column.builder()
              .name(field.getName())
              .unique(isUnique)
              .primaryKey(isPrimaryKey)
              .incrementalKey(isIncrementalKey)
              .type(field.getType())
              .build());
    }

    Map<String, String> metaData = schema.getCustomMetadata();
    String name = metaData.get(CQ_TABLE_NAME);
    String title = metaData.get(CQ_TABLE_TITLE);
    String description = metaData.get(CQ_TABLE_DESCRIPTION);
    String parent = metaData.get(CQ_TABLE_DEPENDS_ON);
    String constraintName = metaData.get(CQ_EXTENSION_CONSTRAINT_NAME);

    TableBuilder tableBuilder =
        Table.builder().name(name).constraintName(constraintName).columns(columns);

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

  public static ByteString encode(Resource resource) throws IOException {
    try (BufferAllocator bufferAllocator = new RootAllocator()) {
      Table table = resource.getTable();
      Schema schema = toArrowSchema(table);
      try (VectorSchemaRoot vectorRoot = VectorSchemaRoot.create(schema, bufferAllocator)) {
        for (int i = 0; i < table.getColumns().size(); i++) {
          FieldVector vector = vectorRoot.getVector(i);
          Object data = resource.getData().get(i).get();
          setVectorData(vector, data);
        }
        // TODO: Support encoding multiple resources
        vectorRoot.setRowCount(1);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
          try (ArrowStreamWriter writer =
              new ArrowStreamWriter(vectorRoot, null, Channels.newChannel(out))) {
            writer.start();
            writer.writeBatch();
            writer.end();
            return ByteString.copyFrom(out.toByteArray());
          }
        }
      }
    }
  }
}
