package io.cloudquery.server;

import io.cloudquery.memdb.MemDB;
import io.cloudquery.server.PluginServe.PluginServeBuilder;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

public class PluginPackageTest {
  @Test
  public void packageTest() {
    Path file = Paths.get("..").toAbsolutePath().normalize();
    String absolutePath = file.toString();

    PluginServe pluginServe =
        new PluginServeBuilder()
            .plugin(new MemDB())
            .args(
                new String[] {
                  "package", "--log-level", "debug", "-m", "initial version", "v1.0.0", absolutePath
                })
            .build();
    int exitCode = pluginServe.Serve();
    assert exitCode == 0;
  }
}
