package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;

import java.nio.ByteBuffer;
import java.util.Objects;

public class UUID extends Scalar<java.util.UUID> {
    private static final int BYTE_WIDTH = 16;
    private static final FixedSizeBinary dt = new FixedSizeBinary(BYTE_WIDTH);

    public UUID() {
        super();
    }

    public UUID(Object value) throws ValidationException {
        super(value);
    }

    @Override
    public ArrowType dataType() {
        return dt;
    }

    @Override
    public void setValue(Object value) throws ValidationException {
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
    public final int hashCode() {
        return Objects.hash(value);
    }
}
