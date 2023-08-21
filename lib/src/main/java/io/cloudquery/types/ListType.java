package io.cloudquery.types;

import java.util.Objects;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class ListType extends ArrowType.List {

  public static ListType listOf(ArrowType elementType) {
    return new ListType(elementType);
  }

  private final ArrowType elementType;

  public ListType(ArrowType elementType) {
    this.elementType = elementType;
  }

  public ArrowType getElementType() {
    return elementType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ListType listType = (ListType) o;
    return Objects.equals(elementType, listType.elementType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), elementType);
  }

  @Override
  public String toString() {
    return "ListType{" + "elementType=" + elementType + '}';
  }
}
