package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class LargeBinary extends Binary {
    @Override
    public ArrowType DataType() {
        return ArrowType.LargeBinary.INSTANCE;
    }
}
