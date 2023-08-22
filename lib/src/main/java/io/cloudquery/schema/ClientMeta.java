package io.cloudquery.schema;

import io.cloudquery.messages.WriteMessage;

public interface ClientMeta {
  String getId();

  void write(WriteMessage message);
}
