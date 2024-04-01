package io.cloudquery.server;

import io.cloudquery.plugin.Plugin;
import io.cloudquery.types.Extensions;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import picocli.CommandLine;

@Builder(access = AccessLevel.PUBLIC)
public class PluginServe {
  @NonNull private final Plugin plugin;
  @Builder.Default private String[] args = new String[] {};

  public int Serve() {
    Extensions.registerExtensions();
    CommandLine cli = new CommandLine(new RootCommand());
    cli.addSubcommand(new ServeCommand(plugin));
    cli.addSubcommand(new PackageCommand(plugin));
    return cli.execute(args);
  }
}
