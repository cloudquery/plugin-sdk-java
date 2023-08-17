package io.cloudquery.scalar;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.time.*;

public class Timestamp extends Scalar<ZonedDateTime> {
    public static final ZoneId zoneID = ZoneOffset.UTC;

    // TODO: add more units support later
    private static final ArrowType dt = new ArrowType.Timestamp(TimeUnit.MILLISECOND, zoneID.toString());

    public Timestamp() {
        super();
    }

    public Timestamp(Object value) throws ValidationException {
        super(value);
    }

    @Override
    public ArrowType dataType() {
        return dt;
    }

    @Override
    public void setValue(Object value) throws ValidationException {
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
}
