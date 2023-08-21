package io.cloudquery.scalar;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class Duration extends Scalar<java.time.Duration> {
  // TODO: add more units support later
  private static final ArrowType dt = new ArrowType.Duration(TimeUnit.MILLISECOND);

  public Duration() {
    super();
  }

  public Duration(Object value) throws ValidationException {
    super(value);
  }

  @Override
  public ArrowType dataType() {
    return dt;
  }

  @Override
  public void setValue(Object value) throws ValidationException {
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

    throw new ValidationException(
        ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
  }
}
