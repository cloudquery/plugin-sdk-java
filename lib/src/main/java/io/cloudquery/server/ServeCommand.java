package io.cloudquery.server;

import static picocli.CommandLine.Option;

import io.cloudquery.internal.servers.discovery.v1.DiscoverServer;
import io.cloudquery.internal.servers.plugin.v3.PluginServer;
import io.cloudquery.plugin.Plugin;
import io.cloudquery.server.AddressConverter.Address;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import lombok.ToString;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.logging.log4j.core.layout.PatternLayout;
import picocli.CommandLine.Command;

@Command(name = "serve", description = "start plugin gRPC server")
@ToString
public class ServeCommand implements Callable<Integer> {
  private static Logger logger;
  public static final List<Integer> DISCOVERY_VERSIONS = List.of(3);

  @Option(
      names = "--address",
      converter = AddressConverter.class,
      description =
          "address to serve on. can be tcp: localhost:7777 or unix socket: `/tmp/plugin.rpc.sock` (default \"${DEFAULT-VALUE}\")")
  private Address address = new Address("localhost", 7777);

  @Option(
      names = "--log-format",
      description = "log format. one of: text,json (default \"${DEFAULT-VALUE}\")")
  private String logFormat = "text";

  @Option(
      names = "--log-level",
      description = "log level. one of: trace,debug,info,warn,error (default \"${DEFAULT-VALUE}\")")
  private String logLevel = "info";

  @Option(
      names = "--network",
      description =
          "the network must be \"tcp\", \"tcp4\", \"tcp6\", \"unix\" or \"unixpacket\" (default \"${DEFAULT-VALUE}\")")
  private String network = "tcp";

  @Option(names = "--disable-sentry", description = "disable sentry")
  private Boolean disableSentry = false;

  @Option(names = "--otel-endpoint", description = "Open Telemetry HTTP collector endpoint")
  private String otelEndpoint = "";

  @Option(
      names = "--otel-endpoint-insecure",
      description = "use Open Telemetry HTTP endpoint (for development only)")
  private Boolean otelEndpointInsecure = false;

  private final Plugin plugin;

  public ServeCommand(Plugin plugin) {
    this.plugin = plugin;
  }

  private LoggerContext initLogger() {
    ConsoleAppender appender =
        ConsoleAppender.createDefaultAppenderForLayout(
            this.logFormat == "text"
                ? PatternLayout.createDefaultLayout()
                : JsonLayout.createDefaultLayout());

    Configuration configuration = ConfigurationFactory.newConfigurationBuilder().build();
    configuration.addAppender(appender);
    LoggerConfig loggerConfig = new LoggerConfig("io.cloudquery", Level.getLevel(logLevel), false);
    loggerConfig.addAppender(appender, null, null);
    configuration.addLogger("io.cloudquery", loggerConfig);
    LoggerContext context = new LoggerContext(ServeCommand.class.getName() + "Context");
    context.start(configuration);

    logger = context.getLogger(ServeCommand.class.getName());
    return context;
  }

  @Override
  public Integer call() {
    try (LoggerContext context = this.initLogger()) {
      Server server =
          Grpc.newServerBuilderForPort(address.port(), InsecureServerCredentials.create())
              .addService(new DiscoverServer(DISCOVERY_VERSIONS))
              .addService(new PluginServer(plugin))
              .addService(ProtoReflectionService.newInstance())
              .executor(Executors.newFixedThreadPool(10))
              .build();
      server.start();
      logger.info("Started server on {}:{}", address.host(), address.port());
      server.awaitTermination();
      return 0;
    } catch (IOException | InterruptedException e) {
      logger.error("Failed to start server", e);
      return 1;
    }
  }
}
