package io.cloudquery.schema;

public class SchemaException extends Exception {
  public SchemaException() {}

  public SchemaException(String message) {
    super(message);
  }

  public SchemaException(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaException(Throwable cause) {
    super(cause);
  }

  public SchemaException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
