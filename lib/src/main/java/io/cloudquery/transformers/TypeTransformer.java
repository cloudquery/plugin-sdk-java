package io.cloudquery.transformers;

import io.cloudquery.types.InetType;
import io.cloudquery.types.JSONType;
import io.cloudquery.types.ListType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.lang.reflect.Field;

public interface TypeTransformer {
    class DefaultTypeTransformer implements TypeTransformer {
        @Override
        public ArrowType transform(Field field) throws TransformerException {
            return transformArrowType(field.getName(), field.getType());
        }

        private static ArrowType transformArrowType(String name, Class<?> type) throws TransformerException {
            switch (type.getName()) {
                case "java.lang.String" -> {
                    return ArrowType.Utf8.INSTANCE;
                }
                case "java.lang.Boolean", "boolean" -> {
                    return ArrowType.Bool.INSTANCE;
                }
                case "java.lang.Integer", "int", "java.lang.Long", "long" -> {
                    return new ArrowType.Int(64, true);
                }
                case "float", "double", "java.lang.Float", "java.lang.Double" -> {
                    return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                }
                case "java.util.Map" -> {
                    return JSONType.INSTANCE;
                }
                case "java.net.InetAddress" -> {
                    return InetType.INSTANCE;
                }
                case "java.time.LocalDateTime" -> {
                    return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
                }
                default -> {
                    if (type.isArray()) {
                        Class<?> componentType = type.getComponentType();
                        if (componentType.getName().equals("byte")) {
                            return ArrowType.Binary.INSTANCE;
                        }
                        return ListType.listOf(transformArrowType(name, componentType));
                    }
                    if (!type.isPrimitive()) {
                        return JSONType.INSTANCE;
                    }
                }
            }
            throw new TransformerException("Unsupported type: " + type.getName() + " for field: " + name);
        }
    }

    ArrowType transform(Field field) throws TransformerException;
}
