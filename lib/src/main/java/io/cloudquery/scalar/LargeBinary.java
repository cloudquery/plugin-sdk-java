package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class LargeBinary extends Binary {

    public LargeBinary() {
    }

    public LargeBinary(Object value) throws ValidationException {
        this.set(value);
    }

    @Override
    public ArrowType dataType() {
        return ArrowType.LargeBinary.INSTANCE;
    }
}
