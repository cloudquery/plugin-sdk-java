package io.cloudquery.plugin;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class NewClientOptions {
  private final boolean noConnection;
}
