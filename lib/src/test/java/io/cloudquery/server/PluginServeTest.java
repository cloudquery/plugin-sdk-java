package io.cloudquery.server;

import io.cloudquery.memdb.MemDB;
import io.cloudquery.plugin.Plugin;
import io.cloudquery.server.PluginServe.PluginServeBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled(value = "blocking tests - only used manually to test the gRPC runs correctly")
public class PluginServeTest {
    public static final String URL = "https://sentry.url";

    private Plugin plugin;

    @BeforeEach
    public void setUp() {
        plugin = new MemDB();
    }

    @Test
    public void simpleCallToServe() {
        PluginServe pluginServe = new PluginServeBuilder().plugin(plugin).args(new String[] { "serve" }).build();
        pluginServe.Serve();
    }

    @Test
    public void simpleCallToServeHelp() {
        PluginServe pluginServe = new PluginServeBuilder().plugin(plugin).args(new String[] { "serve", "--help" })
                .build();
        pluginServe.Serve();
    }

    @Test
    public void simpleOverrideCommandLineArguments() {
        String[] args = new String[] {
                "serve",
                "--address", "foo.bar.com:7777",
                "--disable-sentry",
                "--otel-endpoint", "some-endpoint"
        };
        PluginServe pluginServe = new PluginServeBuilder().plugin(plugin).args(args).build();
        pluginServe.Serve();
    }
}