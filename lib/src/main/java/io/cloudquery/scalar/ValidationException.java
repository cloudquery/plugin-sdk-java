package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class ValidationException extends Exception {
    public Throwable cause;
    public String message;
    public ArrowType type;
    private final Object value;

    static final String NO_CONVERSION_AVAILABLE = "no conversion available";


    ValidationException(Throwable cause, String message, ArrowType type, Object value) {
        super(message, cause);
        this.cause = cause;
        this.message = message;
        this.type = type;
        this.value = value;
    }

    ValidationException(String message, ArrowType type, Object value) {
        super(message);
        this.message = message;
        this.type = type;
        this.value = value;
    }

    public String Error() {
        if (this.cause == null) {
            return String.format("cannot set `%s` with value `%s`: %s", this.type, this.value, this.message);
        }
        return String.format("cannot set `%s` with value `%s`: %s (%s)", this.type, this.value, this.message, this.cause);
    }

    public String Masked() {
        if (this.cause == null) {
            return String.format("cannot set `%s`: %s", this.type.toString(), this.message);
        }
        return String.format("cannot set `%s`: %s (%s)", this.type.toString(), this.message, this.cause);
    }
}
