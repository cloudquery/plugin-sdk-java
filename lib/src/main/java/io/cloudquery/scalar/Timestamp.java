package io.cloudquery.scalar;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class Timestamp extends Scalar<Long> {
  public static final ZoneId zoneID = ZoneOffset.UTC;

  // TODO: add more units support later
  public static final ArrowType dt =
      new ArrowType.Timestamp(TimeUnit.MILLISECOND, zoneID.toString());

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
      this.value = timestamp.withZoneSameInstant(zoneID).toEpochSecond() * 1000;
      return;
    }

    if (value instanceof LocalDate date) {
      this.value = date.atStartOfDay(zoneID).toEpochSecond() * 1000;
      return;
    }

    if (value instanceof LocalDateTime date) {
      this.value = date.atZone(zoneID).toEpochSecond() * 1000;
      return;
    }

    if (value instanceof Integer integer) {
      this.value =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(integer), ZoneOffset.UTC).toEpochSecond()
              * 1000;
      return;
    }

    if (value instanceof Long longValue) {
      this.value =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(longValue), ZoneOffset.UTC).toEpochSecond()
              * 1000;
      return;
    }

    if (value instanceof CharSequence sequence) {
      this.value = ZonedDateTime.parse(sequence).toInstant().toEpochMilli();
      return;
    }

    throw new ValidationException(
        ValidationException.NO_CONVERSION_AVAILABLE, this.dataType(), value);
  }

  @Override
  public java.lang.String toString() {
    if (this.value != null) {
      return ZonedDateTime.ofInstant(Instant.ofEpochMilli((Long) this.value), zoneID).toString();
    }

    return NULL_VALUE_STRING;
  }
}
