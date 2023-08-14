package io.cloudquery.types;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class InetType extends ArrowType.ExtensionType {
    public static final InetType INSTANCE = new InetType();

    @Override
    public ArrowType storageType() {
        return Binary.INSTANCE;
    }

    @Override
    public String extensionName() {
        return "inet";
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
        if (!(other instanceof InetType))
            return false;
        return true;
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
}
