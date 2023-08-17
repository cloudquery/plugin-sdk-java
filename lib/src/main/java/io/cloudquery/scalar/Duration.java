package io.cloudquery.scalar;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class Duration implements Scalar<java.time.Duration> {
    protected java.time.Duration value;

    // TODO: add more units support later
    private static final ArrowType dt = new ArrowType.Duration(TimeUnit.MILLISECOND);

    public Duration() {
    }

    public Duration(Object value) throws ValidationException {
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
        return dt;
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

            if (scalar instanceof Duration duration) {
                this.value = duration.value;
                return;
            }

            this.set(scalar.get());
            return;
        }

        if (value instanceof java.time.Duration duration) {
            this.value = duration;
            return;
        }

        if (value instanceof Integer integer) {
            this.value = java.time.Duration.ofMillis(integer);
            return;
        }

        if (value instanceof Long longValue) {
            this.value = java.time.Duration.ofMillis(longValue);
            return;
        }

        if (value instanceof CharSequence sequence) {
            this.value = java.time.Duration.parse(sequence);
            return;
        }

        throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }

    @Override
    public java.time.Duration get() {
        return this.value;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Duration o) {
            if (this.value == null) {
                return o.value == null;
            }
            return this.value.equals(o.value);
        }
        return false;
    }
}
