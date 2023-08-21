package io.cloudquery.server;

import io.cloudquery.plugin.Plugin;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import picocli.CommandLine;

@Builder(access = AccessLevel.PUBLIC)
public class PluginServe {
  @NonNull private final Plugin plugin;
  @Builder.Default private String[] args = new String[] {};

  public int Serve() {
    return new CommandLine(new RootCommand()).addSubcommand(new ServeCommand(plugin)).execute(args);
  }
}
