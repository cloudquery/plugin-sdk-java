package io.cloudquery.schema;

import io.cloudquery.messages.WriteMessage;

public class NullClient implements ClientMeta {
  @Override
  public String getId() {
    return "null-client";
  }

  @Override
  public void write(WriteMessage message) {
    // No-op for null client
  }
}
