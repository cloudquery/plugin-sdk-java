package io.cloudquery.memdb;

import io.cloudquery.schema.ClientMeta;

public class MemDBClient implements ClientMeta {
  private static final String id = "memdb";

  public MemDBClient() {}

  @Override
  public String getId() {
    return id;
  }

  public void close() {
    // do nothing
  }
}
