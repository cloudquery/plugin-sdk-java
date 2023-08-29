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
import java.util.List;
import java.util.Map;
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
                      .name("string_column1")
                      .type(ArrowType.Utf8.INSTANCE)
                      .unique(true)
                      .incrementalKey(true)
                      .primaryKey(true)
                      .build(),
                  Column.builder().name("string_column2").type(ArrowType.Utf8.INSTANCE).build(),
                  Column.builder().name("boolean_column").type(ArrowType.Bool.INSTANCE).build()))
          .build();

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
            Field.nullable("string_column2", ArrowType.Utf8.INSTANCE));

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
    resource.set("boolean_column", true);

    Assertions.assertDoesNotThrow(
        () -> {
          ByteString byteString = ArrowHelper.encode(resource);
          ArrowHelper.decodeResource(byteString);
        });
  }
}
