package io.cloudquery.scalar;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.time.*;

public class Timestamp implements Scalar<ZonedDateTime> {
    public static final ZoneId zoneID = ZoneOffset.UTC;

    // TODO: add more units support later
    private static final ArrowType dt = new ArrowType.Timestamp(TimeUnit.MILLISECOND, zoneID.toString());

    protected ZonedDateTime value;

    public Timestamp() {
    }

    public Timestamp(Object value) throws ValidationException {
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
    public ArrowType dataType() {
        return dt;
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

            if (scalar instanceof Timestamp Timestamp) {
                this.value = Timestamp.value;
                return;
            }

            this.set(scalar.get());
            return;
        }

        if (value instanceof ZonedDateTime timestamp) {
            this.value = timestamp.withZoneSameInstant(zoneID);
            return;
        }

        if (value instanceof LocalDate date) {
            this.value = date.atStartOfDay(zoneID);
            return;
        }

        if (value instanceof LocalDateTime date) {
            this.value = date.atZone(zoneID);
            return;
        }

        if (value instanceof Integer integer) {
            this.value = ZonedDateTime.ofInstant(Instant.ofEpochMilli(integer), ZoneOffset.UTC);
            return;
        }

        if (value instanceof Long longValue) {
            this.value = ZonedDateTime.ofInstant(Instant.ofEpochMilli(longValue), ZoneOffset.UTC);
            return;
        }

        if (value instanceof CharSequence sequence) {
            this.value = ZonedDateTime.parse(sequence);
            return;
        }

        throw new ValidationException(ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }

    @Override
    public ZonedDateTime get() {
        return this.value;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Timestamp o) {
            if (this.value == null) {
                return o.value == null;
            }
            return this.value.equals(o.value);
        }
        return false;
    }
}
