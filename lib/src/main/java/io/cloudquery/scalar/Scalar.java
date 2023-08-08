package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

public interface Scalar {
    String String();

    Boolean IsValid();

    ArrowType DataType();

    void Set(Object obj);

    Object Get();

    Boolean Equal(Scalar other);
}
