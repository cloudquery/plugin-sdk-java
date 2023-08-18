package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class String extends Scalar<java.lang.String> {

    public String() {
        super();
    }

    public String(Object value) throws ValidationException {
        super(value);
    }

    @Override
    public ArrowType dataType() {
        return ArrowType.Utf8.INSTANCE;
    }

    @Override
    public void setValue(Object value) throws ValidationException {
        this.value = value.toString();
    }
}
