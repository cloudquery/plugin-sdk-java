package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

public interface Scalar<T> {
    String toString();

    boolean isValid();

    ArrowType dataType();

    void set(Object value) throws ValidationException;

    T get();

    boolean equals(Object other);

    String NULL_VALUE_STRING = "(null)";
}
