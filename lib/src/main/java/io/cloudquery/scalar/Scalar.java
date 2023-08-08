package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

public interface Scalar {
    String toString();

    boolean isValid();

    ArrowType dataType();

    void set(Object value) throws ValidationException;

    Object get();

    boolean equals(Object other);

    String NULL_VALUE_STRING = "(null)";
}
