package io.cloudquery.schema;

import io.cloudquery.plugin.TableOutputStream;

public interface TableResolver {
  void resolve(ClientMeta clientMeta, Resource parent, TableOutputStream stream);
}
