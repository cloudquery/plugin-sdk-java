package io.cloudquery.plugin;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class BuildTarget {
  @NonNull protected final BuildOS os;
  @NonNull protected final BuildArch arch;
}
