package io.cloudquery.internal.servers.plugin.v3;

import io.cloudquery.plugin.v3.PluginGrpc.PluginImplBase;
import io.cloudquery.plugin.Plugin;

public class PluginServer extends PluginImplBase {
    private final Plugin plugin;

    public PluginServer(Plugin plugin) {
        this.plugin = plugin;
    }
}
