package io.cloudquery.scalar;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class DateDay implements Scalar<Integer> {
    protected Integer value;

    public DateDay() {
    }

    public DateDay(Object value) throws ValidationException {
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
        return new ArrowType.Date(DateUnit.DAY);
    }

    @Override
    public void set(Object value) throws ValidationException {
        if (value == null) {
            this.value = 0;
            return;
        }

        if (value instanceof Scalar<?> scalar) {
            if (!scalar.isValid()) {
                this.value = 0;
                return;
            }

            if (scalar instanceof DateDay date) {
                this.value = date.value;
                return;
            }

            this.set(scalar.get());
            return;
        }

        if (value instanceof Integer b) {
            this.value = b;
            return;
        }

        if (value instanceof CharSequence sequence) {
            this.value = Integer.parseInt(sequence.toString());
            return;
        }

        throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }

    @Override
    public Integer get() {
        return this.value;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof DateDay o) {
            if (this.value == null) {
                return o.value == null;
            }
            return this.value.equals(o.value);
        }
        return false;
    }
}
