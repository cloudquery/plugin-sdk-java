package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.codec.binary.Base64;

import java.util.Arrays;

public class Binary implements Scalar<byte[]> {
    protected byte[] value;

    public Binary() {
    }

    public Binary(Object value) throws ValidationException {
        this.set(value);
    }

    @Override
    public String toString() {
        if (this.value != null) {
            return Base64.encodeBase64String(this.value);
        }
        return NULL_VALUE_STRING;
    }

    @Override
    public boolean isValid() {
        return this.value != null;
    }

    @Override
    public ArrowType dataType() {
        return ArrowType.Binary.INSTANCE;
    }

    @Override
    public void set(Object value) throws ValidationException {
        if (value == null) {
            this.value = null;
            return;
        }

        if (value instanceof Scalar<?> scalar) {
            if (!scalar.isValid()) {
                this.value = null;
                return;
            }

            this.set(scalar.get());
            return;
        }

        if (value instanceof byte[] bytes) {
            this.value = bytes;
            return;
        }

        if (value instanceof CharSequence sequence) {
            this.value = Base64.decodeBase64(sequence.toString());
            return;
        }

        if (value instanceof char[] chars) {
            this.value = Base64.decodeBase64(new String(chars));
            return;
        }

        throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }

    @Override
    public byte[] get() {
        return this.value;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Binary o) {
            if (this.value == null) {
                return o.value == null;
            }
            return Arrays.equals(this.value, o.value);
        }
        return false;
    }
}