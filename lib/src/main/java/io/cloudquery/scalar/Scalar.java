package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

public interface Scalar {
    String toString();

    boolean IsValid();

    ArrowType DataType();

    void Set(Object obj);

    Object Get();

    boolean Equal(Scalar other);
}
