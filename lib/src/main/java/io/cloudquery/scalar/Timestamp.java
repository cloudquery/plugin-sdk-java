package io.cloudquery.scalar;

import java.time.*;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class Timestamp extends Scalar<Long> {
  public static final ZoneId zoneID = ZoneOffset.UTC;

  // TODO: add more units support later
  private static final ArrowType dt = new ArrowType.Timestamp(TimeUnit.SECOND, zoneID.toString());

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
      this.value = timestamp.withZoneSameInstant(zoneID).toEpochSecond();
      return;
    }

    if (value instanceof LocalDate date) {
      this.value = date.atStartOfDay(zoneID).toEpochSecond();
      return;
    }

    if (value instanceof LocalDateTime date) {
      this.value = date.atZone(zoneID).toEpochSecond();
      return;
    }

    if (value instanceof Integer integer) {
      this.value =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(integer), ZoneOffset.UTC).toEpochSecond();
      return;
    }

    if (value instanceof Long longValue) {
      this.value =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(longValue), ZoneOffset.UTC).toEpochSecond();
      return;
    }

    if (value instanceof CharSequence sequence) {
      this.value = ZonedDateTime.parse(sequence).toEpochSecond();
      return;
    }

    throw new ValidationException(
        ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
  }

  @Override
  public java.lang.String toString() {
    if (this.value != null) {
      return ZonedDateTime.ofInstant(Instant.ofEpochSecond((Long) this.value), zoneID).toString();
    }

    return NULL_VALUE_STRING;
  }
}
