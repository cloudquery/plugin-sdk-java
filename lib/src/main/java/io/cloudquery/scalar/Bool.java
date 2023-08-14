package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.codec.binary.Base64;

public class Bool implements Scalar {
    protected boolean value;
    protected boolean valid;

    public Bool() {
    }

    public Bool(Object value) throws ValidationException {
        this.set(value);
    }

    @Override
    public String toString() {
        if (this.valid) {
            return Boolean.toString(this.value);
        }
        return NULL_VALUE_STRING;
    }

    @Override
    public boolean isValid() {
        return this.valid;
    }

    @Override
    public ArrowType dataType() {
        return ArrowType.Bool.INSTANCE;
    }

    @Override
    public void set(Object value) throws ValidationException {
        if (value == null) {
            this.valid = false;
            this.value = false;
            return;
        }

        if (value instanceof Scalar scalar) {
            if (!scalar.isValid()) {
                this.valid = false;
                this.value = false;
                return;
            }

            this.set(scalar.get());
            return;
        }

        if (value instanceof Boolean b) {
            this.valid = true;
            this.value = b;
            return;
        }

        if (value instanceof CharSequence sequence) {
            this.valid = true;
            this.value = Boolean.parseBoolean(sequence.toString());
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

        if (!(other instanceof Bool o)) {
            return false;
        }

        return (this.valid == o.valid) && (this.value == o.value);
    }
}
