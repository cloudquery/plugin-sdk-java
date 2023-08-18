package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Objects;

public abstract class Scalar<T> {
    protected T value;

    public Scalar() {
    }

    public Scalar(Object value) throws ValidationException {
        this.set(value);
    }

    protected abstract void setValue(Object value) throws ValidationException;

    public abstract ArrowType dataType();

    public java.lang.String toString() {
        if (this.value != null) {
            return this.value.toString();
        }
        return NULL_VALUE_STRING;
    }

    public boolean isValid() {
        return this.value != null;
    }

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

        this.setValue(value);
    }

    public T get() {
        return this.value;
    }

    public boolean equals(Object other) {
        if (!(other instanceof Scalar<?> o)) {
            return false;
        }

        if (!o.getClass().equals(this.getClass())) {
            return false;
        }

        if (this.value == null) {
            return o.value == null;
        }

        return this.value.equals(o.value);
    }

    public final int hashCode() {
        return Objects.hash(value);
    }

    public static final java.lang.String NULL_VALUE_STRING = "(null)";
}
