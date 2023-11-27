package io.cloudquery.scalar;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.time.LocalDateTime;

public class DateMilli extends Scalar<Long> {
  public DateMilli() {
    super();
  }

  public DateMilli(Object value) throws ValidationException {
    super(value);
  }

  @Override
  public ArrowType dataType() {
    return new ArrowType.Date(DateUnit.MILLISECOND);
  }

  @Override
  public void setValue(Object value) throws ValidationException {
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

    if (value instanceof LocalDateTime localDateTime) {
      // we actually store only date
      this.value = localDateTime.toLocalDate().toEpochDay();
      return;
    }

    throw new ValidationException(
        ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
  }
}
