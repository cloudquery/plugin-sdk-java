package io.cloudquery.server;

import io.cloudquery.plugin.Plugin;
import io.cloudquery.server.PluginServe.PluginServeBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

@Disabled(value = "blocking tests - only used manually to test the gRPC runs correctly")
public class PluginServeTest {
    public static final String URL = "https://sentry.url";

    private Plugin plugin;

    @BeforeEach
    public void setUp() {
        plugin = Plugin.builder("test-plugin", "0.1.0").build();
    }

    @Test
    public void simpleCallToServe() throws ServerException {
        PluginServe pluginServe = new PluginServeBuilder().
                plugin(plugin).
                sentryDSN(URL).
                args(List.of("serve")).
                build();
        pluginServe.Serve();
    }

    @Test
    public void simpleCallToServeHelp() throws ServerException {
        PluginServe pluginServe = new PluginServeBuilder().
                plugin(plugin).
                sentryDSN(URL).
                args(List.of("serve", "--help")).
                build();
        pluginServe.Serve();
    }

    @Test
    public void simpleOverrideCommandLineArguments() throws ServerException {
        PluginServe pluginServe = new PluginServeBuilder().
                plugin(plugin).
                sentryDSN(URL).
                args(List.of(
                        "serve",
                        "--address", "foo.bar.com:7777",
                        "--disable-sentry",
                        "--otel-endpoint", "some-endpoint"
                )).
                build();
        pluginServe.Serve();
    }
}