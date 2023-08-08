package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Arrays;

public class Binary implements Scalar {
    private byte[] value;
    private boolean valid;
    private static final ArrowType dataType = new org.apache.arrow.vector.types.pojo.ArrowType.Binary();

    @Override
    public String String() {
        return null;
    }

    @Override
    public boolean IsValid() {
        return this.valid;
    }

    @Override
    public ArrowType DataType() {
        return dataType;
    }

    @Override
    public void Set(Object obj) {

    }

    @Override
    public Object Get() {
        if (this.valid) {
            return this.value;
        }
        return null;
    }

    @Override
    public boolean Equal(Scalar other) {
        if (other == null) {
            return false;
        }

        if (!(other instanceof Binary o)) {
            return false;
        }

        return (this.valid && o.valid) && Arrays.equals(this.value, o.value);
    }
}
