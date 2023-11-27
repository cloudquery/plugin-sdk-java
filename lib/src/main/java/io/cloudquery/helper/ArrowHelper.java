package io.cloudquery.helper;

import com.google.protobuf.ByteString;
import io.cloudquery.scalar.ValidationException;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Resource;
import io.cloudquery.schema.Table;
import io.cloudquery.schema.Table.TableBuilder;
import io.cloudquery.types.JSONType.JSONVector;
import io.cloudquery.types.UUIDType.UUIDVector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.*;

import static java.util.Arrays.asList;

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
    if (data == null) {
      vector.setNull(0);
      return;
    }
    if (vector instanceof BigIntVector bigIntVector) {
      bigIntVector.set(0, (long) data);
      return;
    }
    if (vector instanceof BitVector bitVector) {
      bitVector.set(0, (boolean) data ? 1 : 0);
      return;
    }
    if (vector instanceof DateDayVector dayDateVector) {
      dayDateVector.set(0, (int) data);
      return;
    }
    if (vector instanceof DateMilliVector dateMilliVector) {
      dateMilliVector.set(0, (long) data);
      return;
    }
    if (vector instanceof DurationVector durationVector) {
      durationVector.set(0, (long) data);
      return;
    }
    if (vector instanceof FixedSizeBinaryVector fixedSizeBinaryVector) {
      fixedSizeBinaryVector.set(0, (byte[]) data);
      return;
    }
    if (vector instanceof Float4Vector float4Vector) {
      float4Vector.set(0, (float) data);
      return;
    }
    if (vector instanceof Float8Vector float8Vector) {
      float8Vector.set(0, (double) data);
      return;
    }
    if (vector instanceof IntVector intVector) {
      intVector.set(0, (int) data);
      return;
    }
    if (vector instanceof LargeVarBinaryVector largeVarBinaryVector) {
      largeVarBinaryVector.set(0, (byte[]) data);
      return;
    }
    if (vector instanceof LargeVarCharVector largeVarCharVector) {
      largeVarCharVector.set(0, (Text) data);
      return;
    }
    if (vector instanceof SmallIntVector smallIntVector) {
      smallIntVector.set(0, (short) data);
      return;
    }
    if (vector instanceof TimeMicroVector timeMicroVector) {
      timeMicroVector.set(0, (long) data);
      return;
    }
    if (vector instanceof TimeMilliVector timeMilliVector) {
      timeMilliVector.set(0, (int) data);
      return;
    }
    if (vector instanceof TimeNanoVector timeNanoVector) {
      timeNanoVector.set(0, (long) data);
      return;
    }
    if (vector instanceof TimeSecVector timeSecVector) {
      timeSecVector.set(0, (int) data);
      return;
    }
    if (vector instanceof TimeStampVector timeStampVector) {
      timeStampVector.set(0, (long) data);
      return;
    }
    if (vector instanceof TinyIntVector tinyIntVector) {
      tinyIntVector.set(0, (byte) data);
      return;
    }
    if (vector instanceof UInt1Vector uInt1Vector) {
      uInt1Vector.set(0, (byte) data);
      return;
    }
    if (vector instanceof UInt2Vector uInt2Vector) {
      uInt2Vector.set(0, (short) data);
      return;
    }
    if (vector instanceof UInt4Vector uInt4Vector) {
      uInt4Vector.set(0, (int) data);
      return;
    }
    if (vector instanceof UInt8Vector uInt8Vector) {
      uInt8Vector.set(0, (long) data);
      return;
    }
    if (vector instanceof VarBinaryVector varBinaryVector) {
      varBinaryVector.set(0, (byte[]) data);
      return;
    }
    if (vector instanceof VarCharVector vectorCharVector) {
      vectorCharVector.set(0, (Text) data);
      return;
    }
    if (vector instanceof UUIDVector uuidVector) {
      uuidVector.set(0, (java.util.UUID) data);
      return;
    }
    if (vector instanceof JSONVector jsonVector) {
      jsonVector.setSafe(0, (byte[]) data);
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
      metadata.put(CQ_EXTENSION_UNIQUE, Boolean.toString(column.isUnique()));
      metadata.put(CQ_EXTENSION_PRIMARY_KEY, Boolean.toString(column.isPrimaryKey()));
      metadata.put(CQ_EXTENSION_INCREMENTAL, Boolean.toString(column.isIncrementalKey()));
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
      boolean isUnique = Objects.equals(field.getMetadata().get(CQ_EXTENSION_UNIQUE), "true");
      boolean isPrimaryKey =
          Objects.equals(field.getMetadata().get(CQ_EXTENSION_PRIMARY_KEY), "true");
      boolean isIncrementalKey =
          Objects.equals(field.getMetadata().get(CQ_EXTENSION_INCREMENTAL), "true");

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
        Table.builder()
            .name(name)
            .constraintName(constraintName)
            .columns(columns)
            .title(title)
            .description(description);
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

  public static Resource decodeResource(ByteString byteString)
      throws IOException, ValidationException {
    try (BufferAllocator bufferAllocator = new RootAllocator()) {
      try (ArrowStreamReader reader =
          new ArrowStreamReader(byteString.newInput(), bufferAllocator)) {
        VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
        reader.loadNextBatch();
        Resource resource =
            Resource.builder().table(fromArrowSchema(vectorSchemaRoot.getSchema())).build();
        for (int i = 0; i < vectorSchemaRoot.getSchema().getFields().size(); i++) {
          FieldVector vector = vectorSchemaRoot.getVector(i);
          // TODO: We currently only support a single row
          resource.set(vector.getName(), vector.getObject(0));
        }
        return resource;
      }
    }
  }
}
