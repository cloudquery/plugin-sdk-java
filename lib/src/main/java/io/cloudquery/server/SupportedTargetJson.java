package io.cloudquery.server;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

@AllArgsConstructor
@Getter
public class SupportedTargetJson {
  @NonNull private final String os;
  @NonNull private final String arch;
  @NonNull private final String path;
  @NonNull private final String checksum;
}
