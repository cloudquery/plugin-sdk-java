package io.cloudquery.plugin;

public enum BuildArch {
  AMD64("amd64"),
  ARM64("arm64");

  public final String arch;

  private BuildArch(String arch) {
    this.arch = arch;
  }

  public String toString() {
    return this.arch;
  }
}
