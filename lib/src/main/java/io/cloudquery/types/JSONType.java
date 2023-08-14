package io.cloudquery.types;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class JSONType extends ExtensionType {
    public static final JSONType INSTANCE = new JSONType();

    @Override
    public ArrowType storageType() {
        return ArrowType.Binary.INSTANCE;
    }

    @Override
    public String extensionName() {
        return "json";
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
        return false;
    }

    @Override
    public String serialize() {
        return null;
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
        return null;
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        return null;
    }

    @Override
    public int hashCode() {
        return java.util.Arrays.deepHashCode(new Object[]{});
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof JSONType)) {
            return false;
        }
        return true;
    }
}
