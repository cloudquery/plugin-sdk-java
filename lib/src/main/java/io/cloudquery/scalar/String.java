package io.cloudquery.scalar;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;

public class String extends Scalar<Text> {
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
    this.value = new Text(value.toString());
  }
}
