package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class Bool implements Scalar<Boolean> {
    protected Boolean value;

    public Bool() {
    }

    public Bool(Object value) throws ValidationException {
        this.set(value);
    }

    @Override
    public String toString() {
        if (this.value != null) {
            return this.value.toString();
        }
        return NULL_VALUE_STRING;
    }

    @Override
    public boolean isValid() {
        return this.value != null;
    }

    @Override
    public ArrowType dataType() {
        return ArrowType.Bool.INSTANCE;
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

        if (value instanceof Boolean b) {
            this.value = b;
            return;
        }

        if (value instanceof CharSequence sequence) {
            this.value = Boolean.parseBoolean(sequence.toString());
            return;
        }

        throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }

    @Override
    public Boolean get() {
        return this.value;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Bool o) {
            if (this.value == null) {
                return o.value == null;
            }
            return this.value.equals(o.value);
        }
        return false;
    }
}
