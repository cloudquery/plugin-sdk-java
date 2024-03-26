package io.cloudquery.plugin;

public enum BuildOS {
  Windows("windows"),
  Linux("linux"),
  Darwin("darwin");

  public final String os;

  private BuildOS(String os) {
    this.os = os;
  }

  public String toString() {
    return this.os;
  }
}
