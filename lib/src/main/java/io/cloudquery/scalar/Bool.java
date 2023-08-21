package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class Bool extends Scalar<Boolean> {
  public Bool() {
    super();
  }

  public Bool(Object value) throws ValidationException {
    super(value);
  }

  @Override
  public ArrowType dataType() {
    return ArrowType.Bool.INSTANCE;
  }

  @Override
  public void setValue(Object value) throws ValidationException {
    if (value instanceof Boolean b) {
      this.value = b;
      return;
    }

    if (value instanceof CharSequence sequence) {
      this.value = Boolean.parseBoolean(sequence.toString());
      return;
    }

    throw new ValidationException(
        ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
  }
}
