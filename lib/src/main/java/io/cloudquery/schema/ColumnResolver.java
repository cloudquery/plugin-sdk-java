package io.cloudquery.schema;

import io.cloudquery.transformers.TransformerException;

public interface ColumnResolver {
  void resolve(ClientMeta meta, Resource resource, Column column) throws TransformerException;
}
