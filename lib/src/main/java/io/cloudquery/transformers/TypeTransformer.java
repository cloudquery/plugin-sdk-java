package io.cloudquery.transformers;

import io.cloudquery.scalar.Timestamp;
import io.cloudquery.types.InetType;
import io.cloudquery.types.JSONType;
import io.cloudquery.types.ListType;
import io.cloudquery.types.UUIDType;
import java.lang.reflect.Field;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

public interface TypeTransformer {

  ArrowType transform(Field field) throws TransformerException;

  class DefaultTypeTransformer implements TypeTransformer {
    @Override
    public ArrowType transform(Field field) throws TransformerException {
      return transformArrowType(field.getName(), field.getType());
    }

    private static ArrowType transformArrowType(String name, Class<?> type)
        throws TransformerException {
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
          return Timestamp.dt;
        }
        case "java.util.UUID" -> {
          return new UUIDType();
        }
        default -> {
          if (type.isArray()) {
            Class<?> componentType = type.getComponentType();
            if (componentType.getName().equals("byte")) {
              return ArrowType.Binary.INSTANCE;
            }
            // if element type is already json just return JSON rather than a list of JSON
            ArrowType elementType = transformArrowType(name, componentType);
            return elementType == JSONType.INSTANCE ? elementType : ListType.listOf(elementType);
          }
          if (!type.isPrimitive()) {
            return JSONType.INSTANCE;
          }
        }
      }
      throw new TransformerException("Unsupported type: " + type.getName() + " for field: " + name);
    }
  }
}
