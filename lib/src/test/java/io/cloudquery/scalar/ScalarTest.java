package io.cloudquery.scalar;

import io.cloudquery.types.UUIDType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.ZoneOffset;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class ScalarTest {
    @ParameterizedTest
    @MethodSource("testDataSource")
    public void shouldCreateScalarFromArrowType(ArrowType arrowType, Class<? extends Scalar<?>> scalarClazz) {
        ExtensionTypeRegistry.register(new UUIDType());

        assertInstanceOf(scalarClazz, Scalar.fromArrowType(arrowType));
    }

    public static Stream<Arguments> testDataSource() {
        return Stream.of(
                // Timestamp
                Arguments.of(new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneOffset.UTC.toString()), Timestamp.class),

                // String
                Arguments.of(new ArrowType.Utf8(), String.class),
                Arguments.of(new ArrowType.LargeUtf8(), String.class),

                // Binary
                Arguments.of(new ArrowType.Binary(), Binary.class),
                Arguments.of(new ArrowType.LargeBinary(), Binary.LargeBinary.class),

                // Boolean
                Arguments.of(new ArrowType.Bool(), Bool.class),

                // Signed Integers
                Arguments.of(new ArrowType.Int(8, true), Number.Int8.class),
                Arguments.of(new ArrowType.Int(16, true), Number.Int16.class),
                Arguments.of(new ArrowType.Int(32, true), Number.Int32.class),
                Arguments.of(new ArrowType.Int(64, true), Number.Int64.class),

                // Unsigned Integers
                Arguments.of(new ArrowType.Int(8, false), Number.UInt8.class),
                Arguments.of(new ArrowType.Int(16, false), Number.UInt16.class),
                Arguments.of(new ArrowType.Int(32, false), Number.UInt32.class),
                Arguments.of(new ArrowType.Int(64, false), Number.UInt64.class),

                // Float
                // Arguments.of(  new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), Number.Float16.class),
                Arguments.of(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), Number.Float32.class),
                Arguments.of(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), Number.Float64.class),

                // Extension
                Arguments.of(new UUIDType(), UUID.class),

                // Date
                Arguments.of(new ArrowType.Date(DateUnit.DAY), DateDay.class),
                Arguments.of(new ArrowType.Date(DateUnit.MILLISECOND), DateMilli.class),

                // Duration
                Arguments.of(new ArrowType.Duration(TimeUnit.MILLISECOND), Duration.class)
        );
    }
}
