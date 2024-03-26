package io.cloudquery.plugin;

public enum PluginKind {
  Source("source"),
  Destination("destination");

  public final String kind;

  private PluginKind(String kind) {
    this.kind = kind;
  }

  public String toString() {
    return this.kind;
  }
}
