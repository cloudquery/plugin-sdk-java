package io.cloudquery.transformers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cloudquery.transformers.TypeTransformer.DefaultTypeTransformer;
import io.cloudquery.types.InetType;
import io.cloudquery.types.JSONType;
import io.cloudquery.types.ListType;
import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TypeTransformerTest {

  @SuppressWarnings("unused")
  private static class InnerClass {
    private String innerClassStringField;
  }

  @SuppressWarnings("unused")
  private static class SimpleClass {
    private String stringField;

    private boolean booleanField;
    private Boolean booleanObjectField;

    private int intField;
    private Integer integerObjectField;
    private long longField;
    private Long longObjectField;

    private float floatField;
    private Float floatObjectField;
    private double doubleField;
    private Double doubleObjectField;

    private Map<String, String> mapField;

    private InnerClass innerClassObjectField;

    private int[] intArrayField;
    private String[] stringArrayField;

    private LocalDateTime timeField;

    private InetAddress inetField;

    private byte[] byteArrayField;

    private Object[] objectArrayField;
  }

  @ParameterizedTest
  @MethodSource("testArgumentsSource")
  public void shouldTransformFields(String fieldName, ArrowType expectedArrowType)
      throws NoSuchFieldException, TransformerException {
    DefaultTypeTransformer transfomer = new DefaultTypeTransformer();

    ArrowType arrowType = transfomer.transform(SimpleClass.class.getDeclaredField(fieldName));

    assertEquals(expectedArrowType, arrowType);
  }

  public static Stream<Arguments> testArgumentsSource() {
    return Stream.of(
        // Integer arguments
        Arguments.of("intField", new ArrowType.Int(64, true)),
        Arguments.of("integerObjectField", new ArrowType.Int(64, true)),
        Arguments.of("longField", new ArrowType.Int(64, true)),
        Arguments.of("longObjectField", new ArrowType.Int(64, true)),

        // String arguments
        Arguments.of("stringField", ArrowType.Utf8.INSTANCE),

        // Boolean arguments
        Arguments.of("booleanField", ArrowType.Bool.INSTANCE),
        Arguments.of("booleanObjectField", ArrowType.Bool.INSTANCE),

        // Float field
        Arguments.of("floatField", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Arguments.of(
            "floatObjectField", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Arguments.of("doubleField", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Arguments.of(
            "doubleObjectField", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),

        // Map field
        Arguments.of("mapField", JSONType.INSTANCE),

        // Inner class
        Arguments.of("innerClassObjectField", JSONType.INSTANCE),

        // Array field
        Arguments.of("intArrayField", ListType.listOf(new ArrowType.Int(64, true))),
        Arguments.of("stringArrayField", ListType.listOf(ArrowType.Utf8.INSTANCE)),

        // Time
        Arguments.of(
            "timeField", new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneOffset.UTC.getId())),

        // Byte
        Arguments.of("byteArrayField", ArrowType.Binary.INSTANCE),

        // Inet
        Arguments.of("inetField", InetType.INSTANCE),

        // Object array
        Arguments.of("objectArrayField", JSONType.INSTANCE));
  }
}
