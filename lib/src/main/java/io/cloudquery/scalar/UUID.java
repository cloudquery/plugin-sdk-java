package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;

import java.nio.ByteBuffer;
import java.util.Objects;

public class UUID implements Scalar {
    private static final int BYTE_WIDTH = 16;
    private static final FixedSizeBinary dt = new FixedSizeBinary(BYTE_WIDTH);

    private java.util.UUID value;

    public UUID() {
    }

    public UUID(Object value) throws ValidationException {
        this.set(value);
    }

    @Override
    public boolean isValid() {
        return this.value != null;
    }

    @Override
    public ArrowType dataType() {
        return dt;
    }

    @Override
    public void set(Object value) throws ValidationException {
        if (value == null) {
            this.value = null;
            return;
        }

        if (value instanceof Scalar scalar) {
            if (!scalar.isValid()) {
                this.value = null;
                return;
            }

            if (scalar instanceof UUID uuid) {
                this.value = uuid.value;
                return;
            }

            this.set(scalar.get());
            return;
        }

        if (value instanceof java.util.UUID uuid) {
            this.value = uuid;
            return;
        }

        if (value instanceof CharSequence sequence) {
            this.value = java.util.UUID.fromString(sequence.toString());
            return;
        }

        if (value instanceof byte[] b) {
            if (b.length != BYTE_WIDTH) {
                throw new ValidationException("[]byte must be " + BYTE_WIDTH + " bytes to convert to UUID", this.dataType(), b);
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(b);
            long mostSig = byteBuffer.getLong();
            long leastSig = byteBuffer.getLong();
            this.value = new java.util.UUID(mostSig, leastSig);
            return;
        }

        throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }

    @Override
    public Object get() {
        return this.value;
    }

    @Override
    public final boolean equals(Object other) {
        if (other instanceof UUID o) {
            return this.value == o.value || Objects.equals(this.value, o.value);
        }
        return false;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        if (this.value != null) {
            return this.value.toString();
        }
        return NULL_VALUE_STRING;
    }
}
