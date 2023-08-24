package io.cloudquery.schema;

import com.google.common.base.Objects;
import com.google.protobuf.ByteString;
import io.cloudquery.helper.ArrowHelper;
import io.cloudquery.scalar.Scalar;
import io.cloudquery.scalar.ValidationException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
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
    return ArrowHelper.encode(this);
  }

  public void setCqId(UUID value) throws ValidationException {
    int index = table.indexOfColumn(Column.CQ_ID_COLUMN.getName());
    if (index == -1) {
      return;
    }
    this.data.get(index).set(value);
  }

  public void resolveCQId(boolean deterministicCqId)
      throws ValidationException, NoSuchAlgorithmException {
    UUID randomUUID = UUID.randomUUID();
    if (!deterministicCqId) {
      this.setCqId(randomUUID);
      return;
    }

    // Use an array list to support sorting
    ArrayList<String> pks = new ArrayList<>(this.table.primaryKeys());
    boolean cqOnlyPK =
        pks.stream().allMatch((pk) -> Objects.equal(pk, Column.CQ_ID_COLUMN.getName()));
    if (cqOnlyPK) {
      this.setCqId(randomUUID);
      return;
    }

    Collections.sort(pks);
    // Generate uuid v5 (same as sha-1)
    MessageDigest digest = MessageDigest.getInstance("SHA-1");
    for (String pk : pks) {
      digest.update(pk.getBytes(StandardCharsets.UTF_8));
      digest.update(this.get(pk).toString().getBytes(StandardCharsets.UTF_8));
    }

    ByteBuffer byteBuffer = ByteBuffer.wrap(digest.digest());
    long mostSig = byteBuffer.getLong();
    long leastSig = byteBuffer.getLong();
    this.setCqId(new UUID(mostSig, leastSig));
    return;
  }
}
