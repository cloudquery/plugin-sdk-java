package io.cloudquery.transformers;

import io.cloudquery.schema.Column;
import io.cloudquery.schema.Table;
import io.cloudquery.types.InetType;
import io.cloudquery.types.JSONType;
import io.cloudquery.types.ListType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import static org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import static org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import static org.apache.arrow.vector.types.pojo.ArrowType.Int;
import static org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import static org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransformWithClassTest {
    @SuppressWarnings("unused")
    public static final class InnerTestClass {
        private String name;
        private Integer id;
    }

    @SuppressWarnings("unused")
    public static final class SimpleClass {
        private Integer id;
        private String name;
        private InnerTestClass innerTestClass;
    }

    @SuppressWarnings("unused")
    public static final class TestClass {
        private int intCol;
        private Integer intObjectCol;
        private long longCol;
        private Long longObjectCol;
        private String stringCol;
        private float floatCol;
        private Float floatObjectCol;
        private double doubleCol;
        private Double doubleObjectCol;
        private boolean booleanCol;
        private Boolean booleanObjectCol;
        private InnerTestClass jsonCol;
        private int[] intArrayCol;
        private List<Integer> intListCol;
        private String[] stringArrayCol;
        private List<String> stringListCol;
        private InetAddress inetAddressCol;
        private byte[] byteArrayCol;
        private Object[] anyArrayCol;
        private LocalDateTime timeCol;
    }

    public static final List<Column> expectedColumnsTestClass = List.of(
            Column.builder().name("int_col").type(new Int(64, true)).build(),
            Column.builder().name("int_object_col").type(new Int(64, true)).build(),
            Column.builder().name("long_col").type(new Int(64, true)).build(),
            Column.builder().name("long_object_col").type(new Int(64, true)).build(),
            Column.builder().name("string_col").type(Utf8.INSTANCE).build(),
            Column.builder().name("float_col").type(new FloatingPoint(FloatingPointPrecision.DOUBLE)).build(),
            Column.builder().name("float_object_col").type(new FloatingPoint(FloatingPointPrecision.DOUBLE)).build(),
            Column.builder().name("double_col").type(new FloatingPoint(FloatingPointPrecision.DOUBLE)).build(),
            Column.builder().name("double_object_col").type(new FloatingPoint(FloatingPointPrecision.DOUBLE)).build(),
            Column.builder().name("boolean_col").type(Bool.INSTANCE).build(),
            Column.builder().name("boolean_object_col").type(Bool.INSTANCE).build(),
            Column.builder().name("json_col").type(JSONType.INSTANCE).build(),
            Column.builder().name("int_array_col").type(ListType.listOf(new Int(64, true))).build(),
            Column.builder().name("int_list_col").type(JSONType.INSTANCE).build(),
            Column.builder().name("string_array_col").type(ListType.listOf(Utf8.INSTANCE)).build(),
            Column.builder().name("string_list_col").type(JSONType.INSTANCE).build(),
            Column.builder().name("inet_address_col").type(InetType.INSTANCE).build(),
            Column.builder().name("byte_array_col").type(Binary.INSTANCE).build(),
            Column.builder().name("any_array_col").type(JSONType.INSTANCE).build(),
            Column.builder().name("time_col").type(new Timestamp(TimeUnit.MICROSECOND, null)).build()
    );

    public static final List<Column> expectedColumnsSimpleClass = List.of(
            Column.builder().name("id").type(new Int(64, true)).build(),
            Column.builder().name("name").type(Utf8.INSTANCE).build(),
            Column.builder().name("inner_test_class_name").type(Utf8.INSTANCE).build(),
            Column.builder().name("inner_test_class_id").type(new Int(64, true)).build()
    );

    private Table table;

    @BeforeEach
    void setUp() {
        table = Table.builder().name("test_table").build();
    }

    @Test
    public void shouldTransformTableWithDefaultOptions() throws TransformerException {
        TransformWithClass transformer = TransformWithClass.builder(TestClass.class).build();

        transformer.transformTable(table);

        assertColumnsAreEqual(expectedColumnsTestClass, table.getColumns());
    }


    @Test
    public void shouldUnwrapConfiguredFields() throws TransformerException {
        TransformWithClass transformer = TransformWithClass.builder(SimpleClass.class).
                unwrapField("innerTestClass").
                build();

        transformer.transformTable(table);

        assertColumnsAreEqual(expectedColumnsSimpleClass, table.getColumns());
    }

    @Test
    public void shouldConfigureTopLevelPrimaryKey() throws TransformerException {
        TransformWithClass transformer = TransformWithClass.builder(SimpleClass.class).
                pkField("id").
                unwrapField("innerTestClass").
                build();

        transformer.transformTable(table);

        assertColumnsAreEqual(expectedColumnsSimpleClass, table.getColumns());
        assertTrue(table.getColumn("id").isPresent(), "id column not found");
        assertTrue(table.getColumn("id").get().isPrimaryKey(), "id column not primary key");
    }

    @Test
    public void shouldConfigureUnwrappedPrimaryKey() throws TransformerException {
        TransformWithClass transformer = TransformWithClass.builder(SimpleClass.class).
                pkField("innerTestClass.id").
                unwrapField("innerTestClass").
                build();

        transformer.transformTable(table);

        assertColumnsAreEqual(expectedColumnsSimpleClass, table.getColumns());
        assertTrue(table.getColumn("inner_test_class_id").isPresent(), "id column not found");
        assertTrue(table.getColumn("inner_test_class_id").get().isPrimaryKey(), "id column not primary key");
    }

    @Test
    public void shouldThrowAnExceptionIfPrimaryKeysAreMissing() {
        TransformWithClass transformer = TransformWithClass.builder(SimpleClass.class).
                pkField("innerTestClass.id").
                pkField("badPrimaryKey").
                unwrapField("innerTestClass").
                build();

        TransformerException transformerException = assertThrows(TransformerException.class, () -> transformer.transformTable(table));
        assertEquals("failed to create all of the desired primary keys: [badPrimaryKey]", transformerException.getMessage());
    }

    private void assertColumnsAreEqual(List<Column> expectedColumns, List<Column> actualColumns) {
        assertEquals(expectedColumns.size(), actualColumns.size(), "Columns size mismatch");

        // Check column types match
        for (int i = 0; i < actualColumns.size(); i++) {
            assertEquals(expectedColumns.get(i).getType(), actualColumns.get(i).getType(), "Column type mismatch");
        }

        // Check table now has column
        for (Column expectedColumn : expectedColumns) {
            Optional<Column> optionalColumn = table.getColumn(expectedColumn.getName());
            assertTrue(optionalColumn.isPresent(), "Column " + expectedColumn.getName() + " not found");
        }
    }
}
