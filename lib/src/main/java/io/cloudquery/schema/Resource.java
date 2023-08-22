package io.cloudquery.schema;

import com.google.protobuf.ByteString;
import io.cloudquery.helper.ArrowHelper;
import io.cloudquery.scalar.Scalar;
import io.cloudquery.scalar.ValidationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class Resource {
  private Object item;
  private Resource parent;
  private Table table;

  private final List<Scalar<?>> data;

  @Builder(toBuilder = true)
  public Resource(@NonNull Table table, Resource parent, Object item) {
    this.item = item;
    this.parent = parent;
    this.table = table;
    this.data = new ArrayList<>();

    for (Column column : this.table.getColumns()) {
      this.data.add(Scalar.fromArrowType(column.getType()));
    }
  }

  public void set(String columnName, Object value) throws ValidationException {
    int index = table.indexOfColumn(columnName);
    this.data.get(index).set(value);
  }

  public Scalar<?> get(String columnName) {
    int index = table.indexOfColumn(columnName);
    return this.data.get(index);
  }

  public ByteString encode() throws IOException {
    // TODO: Encode data and not only schema
    return ArrowHelper.encode(table);
  }
}
