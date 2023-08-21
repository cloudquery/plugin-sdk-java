package io.cloudquery.scalar;

import io.cloudquery.types.UUIDType;
import java.util.Objects;
import org.apache.arrow.vector.types.pojo.ArrowType;

public abstract class Scalar<T> {
  protected T value;

  public Scalar() {}

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

  public static Scalar<?> fromArrowType(ArrowType arrowType) {
    switch (arrowType.getTypeID()) {
      case Timestamp -> {
        return new Timestamp();
      }
      case Binary -> {
        return new Binary();
      }
      case LargeBinary -> {
        return new Binary.LargeBinary();
      }
      case Bool -> {
        return new Bool();
      }
      case Utf8, LargeUtf8 -> {
        return new String();
      }
      case Int -> {
        return fromIntArrowType((ArrowType.Int) arrowType);
      }
      case FloatingPoint -> {
        return fromFloatArrowType((ArrowType.FloatingPoint) arrowType);
      }
      case Date -> {
        return fromDateArrowType((ArrowType.Date) arrowType);
      }
      case Duration -> {
        return new Duration();
      }
    }

    if (arrowType instanceof ArrowType.ExtensionType extensionType) {
      //noinspection SwitchStatementWithTooFewBranches
      switch (extensionType.extensionName()) {
        case UUIDType.EXTENSION_NAME -> {
          return new UUID();
        }
          // TODO: Add support for these types when scalar available
          // case JSONType.EXTENSION_NAME -> {
          //     return new JSON();
          // }
          // case INETType.EXTENSION_NAME -> {
          //     return new INET();
          // }
      }
    }

    throw new UnsupportedOperationException("Unsupported type: " + arrowType);
  }

  private static Scalar<?> fromIntArrowType(ArrowType.Int intType) {
    Number<? extends java.lang.Number> numberType;
    switch (intType.getBitWidth()) {
      case 8 -> numberType = intType.getIsSigned() ? new Number.Int8() : new Number.UInt8();
      case 16 -> numberType = intType.getIsSigned() ? new Number.Int16() : new Number.UInt16();
      case 32 -> numberType = intType.getIsSigned() ? new Number.Int32() : new Number.UInt32();
      case 64 -> numberType = intType.getIsSigned() ? new Number.Int64() : new Number.UInt64();
      default -> throw new UnsupportedOperationException("Unsupported type: " + intType);
    }
    return numberType;
  }

  private static Scalar<?> fromFloatArrowType(ArrowType.FloatingPoint floatType) {
    Number<? extends java.lang.Number> numberType;
    switch (floatType.getPrecision()) {
      case SINGLE -> numberType = new Number.Float32();
      case DOUBLE -> numberType = new Number.Float64();
      default -> throw new UnsupportedOperationException("Unsupported type: " + floatType);
    }
    return numberType;
  }

  private static Scalar<?> fromDateArrowType(ArrowType.Date dateType) {
    switch (dateType.getUnit()) {
      case DAY -> {
        return new DateDay();
      }
      case MILLISECOND -> {
        return new DateMilli();
      }
    }
    throw new UnsupportedOperationException("Unsupported type: " + dateType);
  }
}
