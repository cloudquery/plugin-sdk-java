package io.cloudquery.types;

import static org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class UUIDType extends ExtensionType {
  public static final int BYTE_WIDTH = 16;
  public static final String EXTENSION_NAME = "uuid";

  @Override
  public ArrowType storageType() {
    return new FixedSizeBinary(BYTE_WIDTH);
  }

  @Override
  public String extensionName() {
    return EXTENSION_NAME;
  }

  @Override
  public boolean extensionEquals(ExtensionType other) {
    return other instanceof UUIDType;
  }

  @Override
  public String serialize() {
    return "uuid-serialized";
  }

  @Override
  public ArrowType deserialize(ArrowType storageType, String serializedData) {
    if (!serializedData.equals("uuid-serialized")) {
      throw new IllegalArgumentException("Type identifier did not match: " + serializedData);
    }
    if (!storageType.equals(storageType())) {
      throw new IllegalArgumentException(
          "invalid storage type for UUIDType: " + storageType.getTypeID());
    }
    return new UUIDType();
  }

  @Override
  public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
    return new UUIDVector(name, allocator, new FixedSizeBinaryVector(name, allocator, BYTE_WIDTH));
  }

  public static class UUIDVector extends ExtensionTypeVector<FixedSizeBinaryVector> {
    public UUIDVector(String name, BufferAllocator allocator, FixedSizeBinaryVector valueVectors) {
      super(name, allocator, valueVectors);
    }

    @Override
    public Object getObject(int index) {
      final ByteBuffer bb = ByteBuffer.wrap(getUnderlyingVector().getObject(index));
      return new UUID(bb.getLong(), bb.getLong());
    }

    @Override
    public int hashCode(int index) {
      return hashCode(index, null);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
      return getUnderlyingVector().hashCode(index, hasher);
    }

    public UUID get(int index) {
      return (UUID) getObject(index);
    }

    public void set(int index, UUID uuid) {
      ByteBuffer bb = ByteBuffer.allocate(BYTE_WIDTH);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      getUnderlyingVector().set(index, bb.array());
    }
  }
}
