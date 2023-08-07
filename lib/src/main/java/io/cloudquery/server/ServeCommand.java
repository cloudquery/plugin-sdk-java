package io.cloudquery.server;

import io.cloudquery.internal.servers.discovery.v1.DiscoverServer;
import io.cloudquery.internal.servers.plugin.v3.PluginServer;
import io.cloudquery.plugin.Plugin;
import io.cloudquery.server.AddressConverter.Address;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.protobuf.services.ProtoReflectionService;
import lombok.ToString;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command
@ToString
public class ServeCommand implements Callable<Integer> {
    private static final Logger logger = Logger.getLogger(ServeCommand.class.getName());
    public static final List<Integer> DISCOVERY_VERSIONS = List.of(3);

    @Option(names = "--address", converter = AddressConverter.class, description = "address to serve on. can be tcp: localhost:7777 or unix socket: `/tmp/plugin.rpc.sock` (default \"${DEFAULT-VALUE}\")")
    private Address address = new Address("localhost", 7777);

    @Option(names = "--log-format", description = "log format. one of: text,json (default \"${DEFAULT-VALUE}\")")
    private String logFormat = "text";

    @Option(names = "--log-level", description = "log level. one of: trace,debug,info,warn,error (default \"${DEFAULT-VALUE}\")")
    private String logLevel = "info";

    @Option(names = "--network", description = "the network must be \"tcp\", \"tcp4\", \"tcp6\", \"unix\" or \"unixpacket\" (default \"${DEFAULT-VALUE}\")")
    private String network = "tcp";

    @Option(names = "--disable-sentry", description = "disable sentry")
    private Boolean disableSentry = false;

    @Option(names = "--otel-endpoint", description = "Open Telemetry HTTP collector endpoint")
    private String otelEndpoint = "";

    @Option(names = "--otel-endpoint-insecure", description = "use Open Telemetry HTTP endpoint (for development only)")
    private Boolean otelEndpointInsecure = false;

    private final Plugin plugin;

    public ServeCommand(Plugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public Integer call() throws Exception {
        // Initialize a logger

        // Configure open telemetry

        // Configure test listener

        // Configure gRPC server
        Server server = Grpc.newServerBuilderForPort(address.port(), InsecureServerCredentials.create()).
                addService(new DiscoverServer(DISCOVERY_VERSIONS)).
                addService(new PluginServer(plugin)).
                addService(ProtoReflectionService.newInstance()).
                executor(Executors.newFixedThreadPool(10)).
                build();

        // Configure sentry

        // Log we are listening on address and port

        // Run gRPC server and block
        server.start();
        logger.log(Level.INFO, "Started server on {0}", address);
        server.awaitTermination();
        return 0;
    }

}
