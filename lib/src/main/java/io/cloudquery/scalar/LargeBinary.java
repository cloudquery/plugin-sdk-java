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
        if (other == null) {
            return false;
        }

        if (!(other instanceof LargeBinary o)) {
            return false;
        }

        return (this.valid == o.valid) && Arrays.equals(this.value, o.value);
    }
}
