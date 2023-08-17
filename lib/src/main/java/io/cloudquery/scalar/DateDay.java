package io.cloudquery.scalar;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class DateDay extends Scalar<Integer> {
    public DateDay() {
        super();
    }

    public DateDay(Object value) throws ValidationException {
        super(value);
    }

    @Override
    public ArrowType dataType() {
        return new ArrowType.Date(DateUnit.DAY);
    }

    @Override
    public void setValue(Object value) throws ValidationException {
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
}
