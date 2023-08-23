package io.cloudquery.messages;

import io.cloudquery.schema.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class WriteMigrateTable extends WriteMessage {
  private Table table;
  private boolean migrateForce;
}
