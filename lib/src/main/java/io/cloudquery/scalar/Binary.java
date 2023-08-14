package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.codec.binary.Base64;

import java.util.Arrays;

public class Binary implements Scalar {
    protected byte[] value;
    protected boolean valid;

    public Binary() {
    }

    public Binary(Object value) throws ValidationException {
        this.set(value);
    }

    @Override
    public String toString() {
        if (this.valid) {
            return Base64.encodeBase64String(this.value);
        }
        return NULL_VALUE_STRING;
    }

    @Override
    public boolean isValid() {
        return this.valid;
    }

    @Override
    public ArrowType dataType() {
        return ArrowType.Binary.INSTANCE;
    }

    @Override
    public void set(Object value) throws ValidationException {
        if (value == null) {
            this.valid = false;
            this.value = null;
            return;
        }

        if (value instanceof Scalar scalar) {
            if (!scalar.isValid()) {
                this.valid = false;
                this.value = null;
                return;
            }

            this.set(scalar.get());
            return;
        }

        if (value instanceof byte[] bytes) {
            this.valid = true;
            this.value = bytes;
            return;
        }

        if (value instanceof CharSequence sequence) {
            this.value = Base64.decodeBase64(sequence.toString());
            return;
        }

        if (value instanceof char[] chars) {
            this.valid = true;
            this.value = Base64.decodeBase64(new String(chars));
            return;
        }

        throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }

    @Override
    public Object get() {
        if (this.valid) {
            return this.value;
        }
        return null;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }

        if (!(other instanceof Binary o)) {
            return false;
        }

        return (this.valid == o.valid) && Arrays.equals(this.value, o.value);
    }
}