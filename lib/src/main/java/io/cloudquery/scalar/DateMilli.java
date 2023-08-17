package io.cloudquery.scalar;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class DateMilli implements Scalar<Long> {
    protected Long value;

    public DateMilli() {
    }

    public DateMilli(Object value) throws ValidationException {
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
        return new ArrowType.Date(DateUnit.MILLISECOND);
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

            if (scalar instanceof DateMilli date) {
                this.value = date.value;
                return;
            }

            this.set(scalar.get());
            return;
        }

        if (value instanceof Long b) {
            this.value = b;
            return;
        }

        if (value instanceof Integer b) {
            this.value = Long.valueOf(b);
            return;
        }

        if (value instanceof CharSequence sequence) {
            this.value = Long.parseLong(sequence.toString());
            return;
        }

        throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }

    @Override
    public Long get() {
        return this.value;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof DateMilli o) {
            if (this.value == null) {
                return o.value == null;
            }
            return this.value.equals(o.value);
        }
        return false;
    }
}
