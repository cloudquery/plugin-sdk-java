package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class ValidationException extends Exception {
  public Throwable cause;
  public java.lang.String message;
  public ArrowType type;
  private final Object value;

  static final java.lang.String NO_CONVERSION_AVAILABLE = "no conversion available";

  ValidationException(Throwable cause, java.lang.String message, ArrowType type, Object value) {
    super(message, cause);
    this.cause = cause;
    this.message = message;
    this.type = type;
    this.value = value;
  }

  ValidationException(java.lang.String message, ArrowType type, Object value) {
    super(message);
    this.message = message;
    this.type = type;
    this.value = value;
  }

  public java.lang.String Error() {
    if (this.cause == null) {
      return java.lang.String.format(
          "cannot set `%s` with value `%s`: %s", this.type, this.value, this.message);
    }
    return java.lang.String.format(
        "cannot set `%s` with value `%s`: %s (%s)",
        this.type, this.value, this.message, this.cause);
  }

  public java.lang.String Masked() {
    if (this.cause == null) {
      return java.lang.String.format("cannot set `%s`: %s", this.type.toString(), this.message);
    }
    return java.lang.String.format(
        "cannot set `%s`: %s (%s)", this.type.toString(), this.message, this.cause);
  }
}
