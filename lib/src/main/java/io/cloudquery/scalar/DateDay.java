package io.cloudquery.scalar;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class DateDay implements Scalar {
    protected int value;
    protected boolean valid;

    public DateDay() {
    }

    public DateDay(Object value) throws ValidationException {
        this.set(value);
    }

    @Override
    public String toString() {
        if (this.valid) {
            return Integer.toString(this.value);
        }
        return NULL_VALUE_STRING;
    }

    @Override
    public boolean isValid() {
        return this.valid;
    }

    @Override
    public ArrowType dataType() {
        return new ArrowType.Date(DateUnit.DAY);
    }

    @Override
    public void set(Object value) throws ValidationException {
        if (value == null) {
            this.valid = false;
            this.value = 0;
            return;
        }

        if (value instanceof Scalar scalar) {
            if (!scalar.isValid()) {
                this.valid = false;
                this.value = 0;
                return;
            }

            if (scalar instanceof DateDay date) {
                this.valid = date.valid;
                this.value = date.value;
                return;
            }

            this.set(scalar.get());
            return;
        }

        if (value instanceof Integer b) {
            this.valid = true;
            this.value = b;
            return;
        }

        if (value instanceof String string) {
            this.valid = true;
            this.value = Integer.parseInt(string);
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

        if (!(other instanceof DateDay o)) {
            return false;
        }

        return (this.valid == o.valid) && (this.value == o.value);
    }
}
