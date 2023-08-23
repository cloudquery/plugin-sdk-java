package io.cloudquery.scalar;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

public class TimestampTest {
  @Test
  public void testNew() {
    assertDoesNotThrow(
        () -> {
          new Timestamp();
        });
  }

  @Test
  public void testNewWithValidParam() {
    assertDoesNotThrow(
        () -> {
          new Timestamp(1);
          new Timestamp("2011-12-03T10:15:30+01:00[Europe/Paris]");
          new Timestamp(ZonedDateTime.now());

          Scalar<?> s = new Timestamp(ZonedDateTime.now());
          new Timestamp(s);
        });
  }

  @Test
  public void testNewWithInvalidParam() {
    assertThrows(
        ValidationException.class,
        () -> {
          new Timestamp(false);
        });
  }

  @Test
  public void testToString() {
    Timestamp timestamp = new Timestamp();
    assertEquals(Scalar.NULL_VALUE_STRING, timestamp.toString());

    assertDoesNotThrow(
        () -> {
          timestamp.set(1);
        });
    assertEquals("1970-01-01T00:00Z", timestamp.toString());

    java.lang.String ts =
        ZonedDateTime.ofInstant(
                Instant.ofEpochSecond(ZonedDateTime.now(ZoneOffset.UTC).toEpochSecond()),
                ZoneOffset.UTC)
            .toString();
    assertDoesNotThrow(
        () -> {
          timestamp.set(ts);
        });
    assertEquals(ts, timestamp.toString());
  }

  @Test
  public void testDataType() {
    Timestamp timestamp = new Timestamp();
    assertEquals(new ArrowType.Timestamp(TimeUnit.SECOND, "Z"), timestamp.dataType());
  }

  @Test
  public void testIsValid() {
    Timestamp timestamp = new Timestamp();
    assertFalse(timestamp.isValid());

    assertDoesNotThrow(
        () -> {
          timestamp.set(1L);
        });
    assertTrue(timestamp.isValid());
  }

  @Test
  public void testSet() {
    Timestamp timestamp = new Timestamp();
    assertDoesNotThrow(
        () -> {
          timestamp.set(1);
          timestamp.set(1L);
          timestamp.set("2011-12-03T10:15:30+01:00[Europe/Paris]");
          timestamp.set(ZonedDateTime.now());

          Scalar<?> s = new Timestamp(ZonedDateTime.now());
          timestamp.set(s);
        });
  }

  @Test
  public void testSetWithInvalidParam() {
    Timestamp timestamp = new Timestamp();
    assertThrows(
        ValidationException.class,
        () -> {
          timestamp.set(false);
        });
  }

  @Test
  public void testGet() {
    Timestamp timestamp = new Timestamp();
    assertFalse(timestamp.isValid());
    assertNull(timestamp.get());

    ZonedDateTime ts = ZonedDateTime.now(ZoneOffset.UTC);
    assertDoesNotThrow(
        () -> {
          timestamp.set(ts);
        });
    assertTrue(timestamp.isValid());
    assertEquals(ts.toEpochSecond(), timestamp.get());

    assertDoesNotThrow(
        () -> {
          timestamp.set(0);
        });
    assertTrue(timestamp.isValid());
    assertEquals(Instant.EPOCH.atZone(ZoneOffset.UTC).toEpochSecond(), timestamp.get());
  }

  @Test
  public void testEquals() {
    Timestamp a = new Timestamp();
    Timestamp b = new Timestamp();
    assertEquals(a, b);
    assertNotEquals(a, null);
    assertNotEquals(a, new Bool()); // we can't cast Bool to Timestamp
    assertNotEquals(null, a);

    assertDoesNotThrow(
        () -> {
          a.set(-1L);
        });
    assertNotEquals(a, b);

    assertDoesNotThrow(
        () -> {
          for (Object obj :
              new Object[] {
                null,
                0,
                0L,
                -1,
                -1L,
                1,
                1L,
                "1970-01-01T00:00:00.001Z",
                Instant.EPOCH.atZone(ZoneOffset.UTC)
              }) {
            a.set(obj);
            assertEquals(a, new Timestamp(obj));
          }
        });
  }
}
