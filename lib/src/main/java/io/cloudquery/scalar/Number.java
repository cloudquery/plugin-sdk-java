package io.cloudquery.scalar;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.joou.UByte;
import org.joou.UInteger;
import org.joou.ULong;
import org.joou.UShort;

public abstract class Number<T> extends Scalar<T> {
  public Number() {}

  public Number(Object value) throws ValidationException {
    this.set(value);
  }

  public static class Int8 extends Number<Byte> {
    protected static final ArrowType dt = new ArrowType.Int(Byte.SIZE, true);

    public Int8() {
      super();
    }

    public Int8(Object value) throws ValidationException {
      super(value);
    }

    @Override
    public ArrowType dataType() {
      return dt;
    }

    @Override
    protected void setValue(Object value) throws ValidationException {
      if (value instanceof CharSequence sequence) {
        this.value = Byte.valueOf(sequence.toString());
        return;
      }

      if (value instanceof java.lang.Number number) {
        this.value = number.byteValue();
        return;
      }

      throw new ValidationException(
          ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }
  }

  public static class Int16 extends Number<Short> {
    protected static final ArrowType dt = new ArrowType.Int(Short.SIZE, true);

    public Int16() {
      super();
    }

    public Int16(Object value) throws ValidationException {
      super(value);
    }

    @Override
    public ArrowType dataType() {
      return dt;
    }

    @Override
    protected void setValue(Object value) throws ValidationException {
      if (value instanceof CharSequence sequence) {
        this.value = Short.valueOf(sequence.toString());
        return;
      }

      if (value instanceof java.lang.Number number) {
        this.value = number.shortValue();
        return;
      }

      throw new ValidationException(
          ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }
  }

  public static class Int32 extends Number<Integer> {
    protected static final ArrowType dt = new ArrowType.Int(Integer.SIZE, true);

    public Int32() {
      super();
    }

    public Int32(Object value) throws ValidationException {
      super(value);
    }

    @Override
    public ArrowType dataType() {
      return dt;
    }

    @Override
    protected void setValue(Object value) throws ValidationException {
      if (value instanceof CharSequence sequence) {
        this.value = Integer.valueOf(sequence.toString());
        return;
      }

      if (value instanceof java.lang.Number number) {
        this.value = number.intValue();
        return;
      }

      throw new ValidationException(
          ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }
  }

  public static class Int64 extends Number<Long> {
    protected static final ArrowType dt = new ArrowType.Int(Long.SIZE, true);

    public Int64() {
      super();
    }

    public Int64(Object value) throws ValidationException {
      super(value);
    }

    @Override
    public ArrowType dataType() {
      return dt;
    }

    @Override
    protected void setValue(Object value) throws ValidationException {
      if (value instanceof CharSequence sequence) {
        this.value = Long.valueOf(sequence.toString());
        return;
      }

      if (value instanceof java.lang.Number number) {
        this.value = number.longValue();
        return;
      }

      throw new ValidationException(
          ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }
  }

  public static class UInt8 extends Number<UByte> {
    protected static final ArrowType dt = new ArrowType.Int(Byte.SIZE, false);

    public UInt8() {
      super();
    }

    public UInt8(Object value) throws ValidationException {
      super(value);
    }

    @Override
    public ArrowType dataType() {
      return dt;
    }

    @Override
    protected void setValue(Object value) throws ValidationException {
      if (value instanceof CharSequence sequence) {
        this.value = UByte.valueOf(sequence.toString());
        return;
      }

      if (value instanceof java.lang.Number number) {
        this.value = UByte.valueOf(number.byteValue());
        return;
      }

      throw new ValidationException(
          ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }
  }

  public static class UInt16 extends Number<UShort> {
    protected static final ArrowType dt = new ArrowType.Int(Short.SIZE, false);

    public UInt16() {
      super();
    }

    public UInt16(Object value) throws ValidationException {
      super(value);
    }

    @Override
    public ArrowType dataType() {
      return dt;
    }

    @Override
    protected void setValue(Object value) throws ValidationException {
      if (value instanceof CharSequence sequence) {
        this.value = UShort.valueOf(sequence.toString());
        return;
      }

      if (value instanceof java.lang.Number number) {
        this.value = UShort.valueOf(number.shortValue());
        return;
      }

      if (value instanceof Character character) {
        this.value = UShort.valueOf(character);
        return;
      }

      throw new ValidationException(
          ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }
  }

  public static class UInt32 extends Number<UInteger> {
    protected static final ArrowType dt = new ArrowType.Int(Integer.SIZE, false);

    public UInt32() {
      super();
    }

    public UInt32(Object value) throws ValidationException {
      super(value);
    }

    @Override
    public ArrowType dataType() {
      return dt;
    }

    @Override
    protected void setValue(Object value) throws ValidationException {
      if (value instanceof CharSequence sequence) {
        this.value = UInteger.valueOf(sequence.toString());
        return;
      }

      if (value instanceof java.lang.Number number) {
        this.value = UInteger.valueOf(number.intValue());
        return;
      }

      throw new ValidationException(
          ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }
  }

  public static class UInt64 extends Number<ULong> {
    protected static final ArrowType dt = new ArrowType.Int(Long.SIZE, false);

    public UInt64() {
      super();
    }

    public UInt64(Object value) throws ValidationException {
      super(value);
    }

    @Override
    public ArrowType dataType() {
      return dt;
    }

    @Override
    protected void setValue(Object value) throws ValidationException {
      if (value instanceof CharSequence sequence) {
        this.value = ULong.valueOf(sequence.toString());
        return;
      }

      if (value instanceof java.lang.Number number) {
        this.value = ULong.valueOf(number.longValue());
        return;
      }

      throw new ValidationException(
          ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }
  }

  public static class Float32 extends Number<Float> {
    protected static final ArrowType dt =
        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);

    public Float32() {
      super();
    }

    public Float32(Object value) throws ValidationException {
      super(value);
    }

    @Override
    public ArrowType dataType() {
      return dt;
    }

    @Override
    protected void setValue(Object value) throws ValidationException {
      if (value instanceof CharSequence sequence) {
        this.value = Float.valueOf(sequence.toString());
        return;
      }

      if (value instanceof java.lang.Number number) {
        this.value = number.floatValue();
        return;
      }

      throw new ValidationException(
          ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }
  }

  public static class Float64 extends Number<Double> {
    protected static final ArrowType dt =
        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

    public Float64() {
      super();
    }

    public Float64(Object value) throws ValidationException {
      super(value);
    }

    @Override
    public ArrowType dataType() {
      return dt;
    }

    @Override
    protected void setValue(Object value) throws ValidationException {
      if (value instanceof CharSequence sequence) {
        this.value = Double.valueOf(sequence.toString());
        return;
      }

      if (value instanceof java.lang.Number number) {
        this.value = number.doubleValue();
        return;
      }

      throw new ValidationException(
          ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }
  }
}
