package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Arrays;

public class LargeBinary extends Binary {

    public LargeBinary() {
        super();
    }

    public LargeBinary(Object value) throws ValidationException {
        super(value);
    }

    @Override
    public ArrowType dataType() {
        return ArrowType.LargeBinary.INSTANCE;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof LargeBinary o) {
            if (this.value == null) {
                return o.value == null;
            }
            return Arrays.equals(this.value, o.value);
        }
        return false;
    }
}
