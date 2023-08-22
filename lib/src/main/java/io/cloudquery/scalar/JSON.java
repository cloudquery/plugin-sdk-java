package io.cloudquery.scalar;

import static io.cloudquery.scalar.ValidationException.NO_CONVERSION_AVAILABLE;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudquery.types.JSONType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class JSON extends Scalar<byte[]> {
  private static final JSONType dt = new JSONType();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public JSON() {
    super();
  }

  public JSON(Object value) throws ValidationException {
    super(value);
  }

  @Override
  protected void setValue(Object value) throws ValidationException {
    if (value instanceof byte[] bytes) {
      if (bytes.length == 0) {
        return;
      }
      if (!isValidJSON(bytes)) {
        throw new ValidationException("invalid json", dt, value);
      }
      this.value = bytes;
    } else if (value instanceof java.lang.String string) {
      set(string.getBytes());
    } else {
      set(parseAsJSONBytes(value));
    }
  }

  @Override
  public ArrowType dataType() {
    return dt;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof JSON o) {
      if (this.value == null) {
        return o.value == null;
      }
      return Arrays.equals(this.value, o.value);
    }
    return super.equals(other);
  }

  @Override
  public java.lang.String toString() {
    if (this.value != null) {
      return new java.lang.String(this.value);
    }
    return NULL_VALUE_STRING;
  }

  private byte[] parseAsJSONBytes(Object value) throws ValidationException {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      OBJECT_MAPPER.writeValue(outputStream, value);
      return outputStream.toByteArray();
    } catch (IOException e) {
      throw new ValidationException(NO_CONVERSION_AVAILABLE, this.dataType(), value);
    }
  }

  private boolean isValidJSON(byte[] bytes) {
    try {
      OBJECT_MAPPER.readTree(bytes);
      return true;
    } catch (IOException ex) {
      return false;
    }
  }
}
