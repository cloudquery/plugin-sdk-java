package io.cloudquery.helper;

import static io.cloudquery.helper.ArrowHelper.CQ_EXTENSION_CONSTRAINT_NAME;
import static io.cloudquery.helper.ArrowHelper.CQ_EXTENSION_INCREMENTAL;
import static io.cloudquery.helper.ArrowHelper.CQ_EXTENSION_PRIMARY_KEY;
import static io.cloudquery.helper.ArrowHelper.CQ_EXTENSION_UNIQUE;
import static io.cloudquery.helper.ArrowHelper.CQ_TABLE_DEPENDS_ON;
import static io.cloudquery.helper.ArrowHelper.CQ_TABLE_DESCRIPTION;
import static io.cloudquery.helper.ArrowHelper.CQ_TABLE_NAME;
import static io.cloudquery.helper.ArrowHelper.CQ_TABLE_TITLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Resource;
import io.cloudquery.schema.Table;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import io.cloudquery.types.JSONType;
import io.cloudquery.types.UUIDType;
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
                  Column.builder().name("date_milli").type(Types.MinorType.DATEMILLI.getType()).build(),
                  Column.builder().name("decimal256").type(ArrowType.Decimal.createDecimal(10,20,256)).build(),
                  Column.builder().name("decimal128").type(ArrowType.Decimal.createDecimal(10,20,128)).build(),
                  Column.builder().name("duration_s").type(new ArrowType.Duration(TimeUnit.SECOND)).build(),
                  Column.builder().name("duration_ms").type(new ArrowType.Duration(TimeUnit.MILLISECOND)).build(),
                  Column.builder().name("duration_us").type(new ArrowType.Duration(TimeUnit.MICROSECOND)).build(),
                  Column.builder().name("duration_ns").type(new ArrowType.Duration(TimeUnit.NANOSECOND)).build(),
                  Column.builder().name("fixed_size_binary_32").type(new ArrowType.FixedSizeBinary(32)).build(),
                  Column.builder().name("float4").type(Types.MinorType.FLOAT4.getType()).build(),
                  Column.builder().name("float8").type(Types.MinorType.FLOAT8.getType()).build(),
                  Column.builder().name("int").type(Types.MinorType.INT.getType()).build(),
                  Column.builder().name("large_varbinary").type(Types.MinorType.LARGEVARBINARY.getType()).build(),
                  Column.builder().name("large_varchar").type(Types.MinorType.LARGEVARCHAR.getType()).build(),
                  Column.builder().name("small_int").type(Types.MinorType.SMALLINT.getType()).build(),
                  Column.builder().name("time_s").type(Types.MinorType.TIMESEC.getType()).build(),
                  Column.builder().name("time_ms").type(Types.MinorType.TIMEMILLI.getType()).build(),
                  Column.builder().name("time_us").type(Types.MinorType.TIMEMICRO.getType()).build(),
                  Column.builder().name("time_ns").type(Types.MinorType.TIMENANO.getType()).build(),
                  Column.builder().name("timestamp_s").type(Types.MinorType.TIMESTAMPSEC.getType()).build(),
                  Column.builder().name("timestamp_ms").type(Types.MinorType.TIMESTAMPMILLI.getType()).build(),
                  Column.builder().name("timestamp_us").type(Types.MinorType.TIMESTAMPMICRO.getType()).build(),
                  Column.builder().name("timestamp_ns").type(Types.MinorType.TIMESTAMPNANO.getType()).build(),
                  Column.builder().name("timestamp_s_tz").type(new ArrowType.Timestamp(TimeUnit.SECOND, ZoneOffset.UTC.getId())).build(),
                  Column.builder().name("timestamp_ms_tz").type(new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneOffset.UTC.getId())).build(),
                  Column.builder().name("timestamp_us_tz").type(new ArrowType.Timestamp(TimeUnit.MICROSECOND, ZoneOffset.UTC.getId())).build(),
                  Column.builder().name("timestamp_ns_tz").type(new ArrowType.Timestamp(TimeUnit.NANOSECOND, ZoneOffset.UTC.getId())).build(),
                  Column.builder().name("tiny_int").type(Types.MinorType.TINYINT.getType()).build(),
                  Column.builder().name("uint1").type(Types.MinorType.UINT1.getType()).build(),
                  Column.builder().name("uint2").type(Types.MinorType.UINT2.getType()).build(),
                  Column.builder().name("uint4").type(Types.MinorType.UINT4.getType()).build(),
                  Column.builder().name("uint8").type(Types.MinorType.UINT8.getType()).build(),
                  Column.builder().name("varbinary").type(Types.MinorType.VARBINARY.getType()).build(),
                  Column.builder().name("varchar").type(Types.MinorType.VARCHAR.getType()).build(),
                  Column.builder().name("json").type(JSONType.INSTANCE).build(),
                  Column.builder().name("uuid").type(UUIDType.INSTANCE).build()
              )
          ).build();

  @Test
  public void testToArrowSchema() {
    Schema arrowSchema = ArrowHelper.toArrowSchema(TEST_TABLE);

    assertEquals(arrowSchema.getFields().get(0).getName(), "string_column1");
    assertEquals(
        arrowSchema.getFields().get(0).getMetadata(),
        Map.of(
            CQ_EXTENSION_UNIQUE,
            "true",
            CQ_EXTENSION_INCREMENTAL,
            "true",
            CQ_EXTENSION_PRIMARY_KEY,
            "true"));
    assertEquals(arrowSchema.getFields().get(1).getName(), "string_column2");
    assertEquals(
        arrowSchema.getFields().get(1).getMetadata(),
        Map.of(
            CQ_EXTENSION_UNIQUE,
            "false",
            CQ_EXTENSION_INCREMENTAL,
            "false",
            CQ_EXTENSION_PRIMARY_KEY,
            "false"));

    assertEquals(arrowSchema.getFields().get(2).getName(), "boolean_column");
    assertEquals(arrowSchema.getFields().get(3).getName(), "date_days_column");

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

    for (int i = 0; i < TEST_TABLE.getColumns().size(); i++) {
      assertEquals(TEST_TABLE.getColumns().get(i).getName(), table.getColumns().get(i).getName());
      assertEquals(TEST_TABLE.getColumns().get(i).getType(), table.getColumns().get(i).getType());
    }
  }

  @Test
  public void testRoundTripResourceEncoding() throws Exception {
    Resource resource = Resource.builder().table(TEST_TABLE).build();
    resource.set("string_column1", "test_data");
    resource.set("string_column2", "test_data2");
    resource.set("date_days_column", (int) LocalDate.parse("2023-11-24").toEpochDay());
    resource.set("boolean_column", true);

    Assertions.assertDoesNotThrow(
        () -> {
          ByteString byteString = ArrowHelper.encode(resource);
          ArrowHelper.decodeResource(byteString);
        });
  }
}
