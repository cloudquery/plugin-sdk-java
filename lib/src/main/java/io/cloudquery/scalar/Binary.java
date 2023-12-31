package io.cloudquery.scalar;

import java.util.Arrays;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.codec.binary.Base64;

public class Binary extends Scalar<byte[]> {
  public Binary() {
    super();
  }

  public Binary(Object value) throws ValidationException {
    super(value);
  }

  @Override
  public java.lang.String toString() {
    if (this.value != null) {
      return Base64.encodeBase64String(this.value);
    }
    return NULL_VALUE_STRING;
  }

  @Override
  public ArrowType dataType() {
    return ArrowType.Binary.INSTANCE;
  }

  @Override
  public void setValue(Object value) throws ValidationException {
    if (value instanceof byte[] bytes) {
      this.value = bytes;
      return;
    }

    if (value instanceof CharSequence sequence) {
      this.value = Base64.decodeBase64(sequence.toString());
      return;
    }

    if (value instanceof char[] chars) {
      this.value = Base64.decodeBase64(new java.lang.String(chars));
      return;
    }

    throw new ValidationException(
        ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Binary o) {
      if (this.value == null) {
        return o.value == null;
      }
      return Arrays.equals(this.value, o.value);
    }
    return false;
  }

  public static class LargeBinary extends Binary {

    public LargeBinary() {
      super();
    }

    public LargeBinary(Object value) throws ValidationException {
      super(value);
    }

    @Override
    public ArrowType dataType() {
      return ArrowType.LargeBinary.INSTANCE;
    }
  }
}
