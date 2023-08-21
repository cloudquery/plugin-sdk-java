package io.cloudquery.plugin;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class BackendOptions {
  private final String tableName;
  private final String connection;
}
