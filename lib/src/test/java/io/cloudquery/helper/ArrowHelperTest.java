package io.cloudquery.helper;

import static io.cloudquery.helper.ArrowHelper.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Resource;
import io.cloudquery.schema.Table;
import io.cloudquery.types.JSONType;
import io.cloudquery.types.UUIDType;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ArrowHelperTest {

  public static final Table TEST_TABLE =
      Table.builder()
          .name("table1")
          .description("A simple test table")
          .title("Test table title")
          .parent(Table.builder().name("parent").build())
          .columns(
              List.of(
                  Column.builder()
                      .name("pk")
                      .type(ArrowType.Utf8.INSTANCE)
                      .unique(true)
                      .incrementalKey(true)
                      .primaryKey(true)
                      .build(),
                  Column.builder().name("big_int").type(Types.MinorType.BIGINT.getType()).build(),
                  Column.builder().name("bit").type(Types.MinorType.BIT.getType()).build(),
                  Column.builder().name("date_day").type(Types.MinorType.DATEDAY.getType()).build(),
                  Column.builder()
                      .name("date_milli")
                      .type(Types.MinorType.DATEMILLI.getType())
                      .build(),
                  Column.builder()
                      .name("duration_s")
                      .type(new ArrowType.Duration(TimeUnit.SECOND))
                      .build(),
                  Column.builder()
                      .name("duration_ms")
                      .type(new ArrowType.Duration(TimeUnit.MILLISECOND))
                      .build(),
                  Column.builder()
                      .name("duration_us")
                      .type(new ArrowType.Duration(TimeUnit.MICROSECOND))
                      .build(),
                  Column.builder()
                      .name("duration_ns")
                      .type(new ArrowType.Duration(TimeUnit.NANOSECOND))
                      .build(),
                  Column.builder().name("float4").type(Types.MinorType.FLOAT4.getType()).build(),
                  Column.builder().name("float8").type(Types.MinorType.FLOAT8.getType()).build(),
                  Column.builder().name("int").type(Types.MinorType.INT.getType()).build(),
                  Column.builder()
                      .name("large_varbinary")
                      .type(Types.MinorType.LARGEVARBINARY.getType())
                      .build(),
                  Column.builder()
                      .name("large_varchar")
                      .type(Types.MinorType.LARGEVARCHAR.getType())
                      .build(),
                  Column.builder()
                      .name("small_int")
                      .type(Types.MinorType.SMALLINT.getType())
                      .build(),
                  Column.builder()
                      .name("timestamp_s")
                      .type(Types.MinorType.TIMESTAMPSEC.getType())
                      .build(),
                  Column.builder()
                      .name("timestamp_ms")
                      .type(Types.MinorType.TIMESTAMPMILLI.getType())
                      .build(),
                  Column.builder()
                      .name("timestamp_us")
                      .type(Types.MinorType.TIMESTAMPMICRO.getType())
                      .build(),
                  Column.builder()
                      .name("timestamp_ns")
                      .type(Types.MinorType.TIMESTAMPNANO.getType())
                      .build(),
                  Column.builder()
                      .name("timestamp_s_tz")
                      .type(new ArrowType.Timestamp(TimeUnit.SECOND, ZoneOffset.UTC.getId()))
                      .build(),
                  Column.builder()
                      .name("timestamp_ms_tz")
                      .type(new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneOffset.UTC.getId()))
                      .build(),
                  Column.builder()
                      .name("timestamp_us_tz")
                      .type(new ArrowType.Timestamp(TimeUnit.MICROSECOND, ZoneOffset.UTC.getId()))
                      .build(),
                  Column.builder()
                      .name("timestamp_ns_tz")
                      .type(new ArrowType.Timestamp(TimeUnit.NANOSECOND, ZoneOffset.UTC.getId()))
                      .build(),
                  Column.builder().name("tiny_int").type(Types.MinorType.TINYINT.getType()).build(),
                  Column.builder().name("uint1").type(Types.MinorType.UINT1.getType()).build(),
                  Column.builder().name("uint2").type(Types.MinorType.UINT2.getType()).build(),
                  Column.builder().name("uint4").type(Types.MinorType.UINT4.getType()).build(),
                  Column.builder().name("uint8").type(Types.MinorType.UINT8.getType()).build(),
                  Column.builder()
                      .name("varbinary")
                      .type(Types.MinorType.VARBINARY.getType())
                      .build(),
                  Column.builder().name("varchar").type(Types.MinorType.VARCHAR.getType()).build(),
                  Column.builder().name("json").type(JSONType.INSTANCE).build(),
                  Column.builder().name("uuid").type(UUIDType.INSTANCE).build()))
          .build();

  @Test
  public void testToArrowSchema() {
    Schema arrowSchema = ArrowHelper.toArrowSchema(TEST_TABLE);

    for (Column col : TEST_TABLE.getColumns()) {
      int idx = TEST_TABLE.indexOfColumn(col.getName());
      Field field = arrowSchema.getFields().get(idx);
      assertEquals(col.getName(), field.getName());
      if (idx == 0) {
        assertEquals(
            Map.of(
                CQ_EXTENSION_UNIQUE,
                "true",
                CQ_EXTENSION_INCREMENTAL,
                "true",
                CQ_EXTENSION_PRIMARY_KEY,
                "true"),
            field.getMetadata());
      } else if (col.getName().equals("json")) {
        assertEquals(
            Map.of(
                CQ_EXTENSION_UNIQUE,
                "false",
                CQ_EXTENSION_INCREMENTAL,
                "false",
                CQ_EXTENSION_PRIMARY_KEY,
                "false",
                ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME,
                "json",
                ArrowType.ExtensionType.EXTENSION_METADATA_KEY_METADATA,
                "json-serialized"),
            field.getMetadata());
      } else if (col.getName().equals("uuid")) {
        assertEquals(
            Map.of(
                CQ_EXTENSION_UNIQUE,
                "false",
                CQ_EXTENSION_INCREMENTAL,
                "false",
                CQ_EXTENSION_PRIMARY_KEY,
                "false",
                ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME,
                "uuid",
                ArrowType.ExtensionType.EXTENSION_METADATA_KEY_METADATA,
                "uuid-serialized"),
            field.getMetadata());
      } else {
        assertEquals(
            Map.of(
                CQ_EXTENSION_UNIQUE,
                "false",
                CQ_EXTENSION_INCREMENTAL,
                "false",
                CQ_EXTENSION_PRIMARY_KEY,
                "false"),
            field.getMetadata());
      }
    }

    assertEquals(
        arrowSchema.getCustomMetadata(),
        Map.of(
            CQ_TABLE_NAME, "table1",
            CQ_TABLE_DESCRIPTION, "A simple test table",
            CQ_TABLE_TITLE, "Test table title",
            CQ_TABLE_DEPENDS_ON, "parent",
            CQ_EXTENSION_CONSTRAINT_NAME, ""));
  }

  @Test
  public void testFromArrowSchema() {
    List<Field> fields =
        List.of(
            Field.nullable("string_column1", ArrowType.Utf8.INSTANCE),
            Field.nullable("string_column2", ArrowType.Utf8.INSTANCE),
            Field.nullable("date_days_column", new ArrowType.Date(DateUnit.DAY)));

    Schema schema = new Schema(fields, Map.of(CQ_TABLE_NAME, "table1"));

    Table table = ArrowHelper.fromArrowSchema(schema);

    assertEquals(table.getName(), "table1");

    for (int i = 0; i < table.getColumns().size(); i++) {
      Column column = table.getColumns().get(i);
      assertEquals(column.getName(), fields.get(i).getName());
      assertEquals(column.getType(), fields.get(i).getType());
    }
  }

  @Test
  public void testRoundTripTableEncoding() throws IOException {
    ByteString byteString = ArrowHelper.encode(TEST_TABLE);
    Table table = ArrowHelper.decode(byteString);

    assertEquals(table.getName(), TEST_TABLE.getName());
    assertEquals(table.getDescription(), TEST_TABLE.getDescription());
    assertEquals(table.getTitle(), TEST_TABLE.getTitle());
    assertEquals(table.getParent().getName(), TEST_TABLE.getParent().getName());
    assertEquals(TEST_TABLE.getColumns().size(), table.getColumns().size());

    for (int i = 0; i < TEST_TABLE.getColumns().size(); i++) {
      Column srcCol = TEST_TABLE.getColumns().get(i);
      Column dstCol = table.getColumns().get(i);
      assertEquals(srcCol.getName(), dstCol.getName());
      assertEquals(srcCol.getType(), dstCol.getType());
    }
  }

  @Test
  public void testRoundTripResourceEncoding() throws Exception {
    Resource resource = Resource.builder().table(TEST_TABLE).build();
    resource.set("pk", "test_pk");
    resource.set("big_int", -1024L);
    resource.set("date_day", (int) LocalDateTime.now().toLocalDate().toEpochDay());
    resource.set("date_milli", LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) * 1000);
    resource.set("duration_s", Duration.ofSeconds(1024));
    resource.set("duration_ms", Duration.ofMillis(1024));
    resource.set("duration_us", Duration.ofNanos(1024000));
    resource.set("duration_ns", Duration.ofNanos(1024));
    resource.set("float4", 5.0F);
    resource.set("float8", 5.0D);
    resource.set("int", -1024);
    resource.set("large_varbinary", "1234");
    resource.set("large_varchar", "1234");
    resource.set("small_int", (short) -1024);
    resource.set("tiny_int", (byte) -100);
    resource.set("uint1", (byte) 100);
    resource.set("uint2", (short) 1024);
    resource.set("uint4", 1024);
    resource.set("uint8", 1024L);
    resource.set("varbinary", "1234");
    resource.set("varchar", "1234");
    resource.set("json", "{\"a\":1234}");
    resource.set("uuid", UUID.randomUUID());

    Assertions.assertDoesNotThrow(
        () -> {
          ByteString byteString = ArrowHelper.encode(resource);
          ArrowHelper.decodeResource(byteString);
        });
  }
}
