package io.cloudquery.schema;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class TableColumnChange {
  private TableColumnChangeType type;
  private String columnName;
  private Column current;
  private Column previous;
}
