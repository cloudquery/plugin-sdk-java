package io.cloudquery.server;

import io.cloudquery.plugin.Plugin;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

@Builder(access = AccessLevel.PUBLIC)
public class PluginServe {
    @NonNull
    private final Plugin plugin;
    @Builder.Default
    private List<String> args = new ArrayList<>();
    private boolean destinationV0V1Server;
    private String sentryDSN;
    private boolean testListener;
    //TODO: Allow a test listener to be passed in
    // 	testListenerConn      *bufconn.Listener

    public void Serve() throws ServerException {
        int exitStatus = new CommandLine(new RootCommand()).
                addSubcommand("serve", new ServeCommand(plugin)).
                addSubcommand("doc", new DocCommand()).
                execute(args.toArray(new String[]{}));
        if (exitStatus != 0) {
            throw new ServerException("error processing command line exit status = "+exitStatus);
        }
    }
}
