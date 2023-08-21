package io.cloudquery;

import io.cloudquery.memdb.MemDB;
import io.cloudquery.server.PluginServe;

public class MainClass {
    public static void main(String[] args) {
        PluginServe serve = PluginServe.builder().plugin(new MemDB()).args(args).build();
        int exitCode = serve.Serve();
        System.exit(exitCode);
    }
}
