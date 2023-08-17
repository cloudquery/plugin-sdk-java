package io.cloudquery.scalar;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.joou.UByte;
import org.joou.UInteger;
import org.joou.ULong;
import org.joou.UShort;

public abstract class Number<T> implements Scalar<T> {
    protected T value;

    protected abstract void setValue(Object value) throws ValidationException;


    public Number() {
    }

    public Number(Object value) throws ValidationException {
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

            this.setValue(scalar.get());
            return;
        }

        throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Number<?> o) {
            if (this.value == null) {
                return o.value == null;
            }
            return this.value.equals(o.value);
        }
        return false;
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
        public Byte get() {
            return this.value;
        }

        @Override
        protected void setValue(Object value) throws ValidationException {
            if (value instanceof CharSequence sequence) {
                this.value = Byte.valueOf(sequence.toString());
            }

            if (value instanceof java.lang.Number number) {
                this.value = number.byteValue();
            }

            throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
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
        public Short get() {
            return this.value;
        }

        @Override
        protected void setValue(Object value) throws ValidationException {
            if (value instanceof CharSequence sequence) {
                this.value = Short.valueOf(sequence.toString());
            }

            if (value instanceof java.lang.Number number) {
                this.value = number.shortValue();
            }

            throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
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
        public Integer get() {
            return this.value;
        }

        @Override
        protected void setValue(Object value) throws ValidationException {
            if (value instanceof CharSequence sequence) {
                this.value = Integer.valueOf(sequence.toString());
            }

            if (value instanceof java.lang.Number number) {
                this.value = number.intValue();
            }

            throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
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
        public Long get() {
            return this.value;
        }

        @Override
        protected void setValue(Object value) throws ValidationException {
            if (value instanceof CharSequence sequence) {
                this.value = Long.valueOf(sequence.toString());
            }

            if (value instanceof java.lang.Number number) {
                this.value = number.longValue();
            }

            throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
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
        public UByte get() {
            return this.value;
        }

        @Override
        protected void setValue(Object value) throws ValidationException {
            if (value instanceof CharSequence sequence) {
                this.value = UByte.valueOf(sequence.toString());
            }

            if (value instanceof java.lang.Number number) {
                this.value = UByte.valueOf(number.byteValue());
            }

            throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
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
        public UShort get() {
            return this.value;
        }

        @Override
        protected void setValue(Object value) throws ValidationException {
            if (value instanceof CharSequence sequence) {
                this.value = UShort.valueOf(sequence.toString());
            }

            if (value instanceof java.lang.Number number) {
                this.value = UShort.valueOf(number.shortValue());
            }

            throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
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
        public UInteger get() {
            return this.value;
        }

        @Override
        protected void setValue(Object value) throws ValidationException {
            if (value instanceof CharSequence sequence) {
                this.value = UInteger.valueOf(sequence.toString());
            }

            if (value instanceof java.lang.Number number) {
                this.value = UInteger.valueOf(number.intValue());
            }

            throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
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
        public ULong get() {
            return this.value;
        }

        @Override
        protected void setValue(Object value) throws ValidationException {
            if (value instanceof CharSequence sequence) {
                this.value = ULong.valueOf(sequence.toString());
            }

            if (value instanceof java.lang.Number number) {
                this.value = ULong.valueOf(number.longValue());
            }

            throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
        }
    }

    public static class Float32 extends Number<Float> {
        protected static final ArrowType dt = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);

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
        public Float get() {
            return this.value;
        }

        @Override
        protected void setValue(Object value) throws ValidationException {
            if (value instanceof CharSequence sequence) {
                this.value = Float.valueOf(sequence.toString());
            }

            if (value instanceof java.lang.Number number) {
                this.value = number.floatValue();
            }

            throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
        }
    }
    public static class Float64 extends Number<Double> {
        protected static final ArrowType dt = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

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
        public Double get() {
            return this.value;
        }

        @Override
        protected void setValue(Object value) throws ValidationException {
            if (value instanceof CharSequence sequence) {
                this.value = Double.valueOf(sequence.toString());
            }

            if (value instanceof java.lang.Number number) {
                this.value = number.doubleValue();
            }

            throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
        }
    }
}
