package io.cloudquery.types;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class JSONType extends ExtensionType {
  public static final JSONType INSTANCE = new JSONType();
  public static final String EXTENSION_NAME = "json";

  @Override
  public ArrowType storageType() {
    return Binary.INSTANCE;
  }

  @Override
  public String extensionName() {
    return EXTENSION_NAME;
  }

  @Override
  public boolean extensionEquals(ExtensionType other) {
    return other instanceof JSONType;
  }

  @Override
  public String serialize() {
    return "json-serialized";
  }

  @Override
  public ArrowType deserialize(ArrowType storageType, String serializedData) {
    if (!serializedData.equals("json-serialized")) {
      throw new IllegalArgumentException("Type identifier did not match: " + serializedData);
    }
    if (!storageType.equals(storageType())) {
      throw new IllegalArgumentException(
          "invalid storage type for JSONType: " + storageType.getTypeID());
    }
    return new JSONType();
  }

  @Override
  public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
    return new JSONVector(name, allocator, new VarBinaryVector(name, allocator));
  }

  @Override
  public int hashCode() {
    return java.util.Arrays.deepHashCode(new Object[] {});
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof JSONType;
  }

  public static class JSONVector extends ExtensionTypeVector<VarBinaryVector> {
    public JSONVector(String name, BufferAllocator allocator, VarBinaryVector underlyingVector) {
      super(name, allocator, underlyingVector);
    }

    @Override
    public Object getObject(int index) {
      return getUnderlyingVector().getObject(index);
    }

    @Override
    public int hashCode(int index) {
      return hashCode(index, null);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
      return getUnderlyingVector().hashCode(index, hasher);
    }

    public String get(int index) {
      return new String((byte[]) getObject(index));
    }

    public void set(int index, String value) {
      getUnderlyingVector().setSafe(index, value.getBytes(), 0, value.getBytes().length);
    }
  }
}
