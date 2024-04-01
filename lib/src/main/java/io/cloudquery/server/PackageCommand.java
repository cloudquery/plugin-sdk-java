package io.cloudquery.server;

import com.google.common.base.Strings;
import io.cloudquery.plugin.BuildTarget;
import io.cloudquery.plugin.NewClientOptions;
import io.cloudquery.plugin.Plugin;
import io.cloudquery.plugin.PluginKind;
import io.cloudquery.schema.Table;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.ToString;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
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
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "package", description = "package the plugin as a Docker image")
@ToString
public class PackageCommand implements Callable<Integer> {
  private static Logger logger;

  @Parameters(index = "0", description = "plugin version to package")
  private String pluginVersion;

  @Parameters(index = "1", description = "plugin directory to package")
  private String pluginDirectory;

  @Option(
      required = true,
      names = {"-m", "--message"},
      description =
          "message that summarizes what is new or changed in this version. Supports markdown.")
  private String message = "";

  @Option(
      names = "--log-format",
      description = "log format. one of: text,json (default \"${DEFAULT-VALUE}\")")
  private String logFormat = "text";

  @Option(
      names = "--log-level",
      description = "log level. one of: trace,debug,info,warn,error (default \"${DEFAULT-VALUE}\")")
  private String logLevel = "info";

  @Option(names = "--no-sentry", description = "disable sentry")
  private Boolean disableSentry = false;

  @Option(names = "--otel-endpoint", description = "Open Telemetry HTTP collector endpoint")
  private String otelEndpoint = "";

  @Option(
      names = "--otel-endpoint-insecure",
      description = "use Open Telemetry HTTP endpoint (for development only)")
  private Boolean otelEndpointInsecure = false;

  @Option(
      names = "--dist-dir",
      description = "dist directory to output the built plugin. (default: <plugin_directory>/dist)")
  private String distDir = "";

  @Option(
      names = "--docs-dir",
      description =
          "docs directory containing markdown files to copy to the dist directory. (default: <plugin_directory>/docs)")
  private String docsDir = "";

  private final Plugin plugin;

  public PackageCommand(Plugin plugin) {
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
    this.plugin.setLogger(logger);
    return context;
  }

  @SuppressWarnings("null")
  @Override
  public Integer call() {
    try (LoggerContext context = this.initLogger()) {
      if (Strings.isNullOrEmpty(plugin.getName())) {
        logger.error("name is required");
        return 1;
      }
      if (Strings.isNullOrEmpty(plugin.getVersion())) {
        logger.error("version is required");
        return 1;
      }
      if (Strings.isNullOrEmpty(plugin.getTeam())) {
        logger.error("team is required");
        return 1;
      }
      if (Strings.isNullOrEmpty(plugin.getDockerfile())) {
        logger.error("Dockerfile is required");
        return 1;
      }
      if (plugin.getBuildTargets() == null || plugin.getBuildTargets().length == 0) {
        logger.error("At least one build target is required");
        return 1;
      }
      if (Strings.isNullOrEmpty(distDir)) {
        distDir = Paths.get(pluginDirectory, "dist").toString();
      }
      if (Strings.isNullOrEmpty(docsDir)) {
        docsDir = Paths.get(pluginDirectory, "docs").toString();
      }

      initDist();
      copyDocs();
      writeTablesJson();
      List<SupportedTargetJson> supportedTargets = buildDocker();
      writePackageJson(supportedTargets);

      return 0;
    } catch (Exception e) {
      logger.error("Failed to package plugin", e);
      return 1;
    }
  }

  private void initDist() throws IOException {
    logger.info("Packaging plugin to {}", distDir);
    File dist = new File(distDir);
    if (!dist.exists()) {
      boolean created = dist.mkdirs();
      if (!created) {
        logger.error("Failed to create dist directory {}", distDir);
        throw new IOException("Failed to create dist directory");
      }
    }
  }

  private void copyDocs() throws IllegalArgumentException, IOException {
    File docs = new File(docsDir);
    if (!docs.exists()) {
      logger.error("Docs directory path{} does not exist", docsDir);
      throw new IllegalArgumentException("Docs directory does not exist");
    }
    if (!docs.isDirectory()) {
      logger.error("Docs path {} is not a directory", docsDir);
      throw new IllegalArgumentException("Docs path is not a directory");
    }

    String outputPath = Paths.get(distDir, "docs").toString();
    logger.info("Copying docs from {} to {}", docsDir, outputPath);
    File output = new File(outputPath);
    FileUtils.copyDirectory(docs, output);
  }

  private void writeTablesJson() throws Exception {
    if (plugin.getKind() != PluginKind.Source) {
      return;
    }

    String outputPath = Paths.get(distDir, "tables.json").toString();
    logger.info("Writing tables.json to {}", outputPath);
    plugin.init("", NewClientOptions.builder().noConnection(true).build());
    List<Table> tables = plugin.tables(Arrays.asList("*"), Arrays.asList(), false);
    List<Table> flattenTables = Table.flattenTables(tables);
    TablesJson tablesJson = new TablesJson(flattenTables);
    String json = tablesJson.toJson();
    FileUtils.writeStringToFile(new File(outputPath), json, "UTF-8");
  }

  @SuppressWarnings("null")
  private List<SupportedTargetJson> buildDocker()
      throws IllegalArgumentException, IOException, InterruptedException {
    String dockerFilePath = Paths.get(pluginDirectory, plugin.getDockerfile()).toString();
    File dockerFile = new File(dockerFilePath);
    if (!dockerFile.exists()) {
      logger.error("Dockerfile {} does not exist", dockerFilePath);
      throw new IllegalArgumentException("Dockerfile does not exist");
    }
    if (!dockerFile.isFile()) {
      logger.error("Dockerfile {} is not a file", dockerFilePath);
      throw new IllegalArgumentException("Dockerfile is not a file");
    }

    List<SupportedTargetJson> supportedTargets = new ArrayList<>();
    for (BuildTarget target : plugin.getBuildTargets()) {
      String imageRepository =
          String.format(
              "docker.cloudquery.io/%s/%s-%s",
              plugin.getTeam(), plugin.getKind(), plugin.getName());
      String os = target.getOs().toString();
      String arch = target.getArch().toString();
      String imageTag = String.format("%s:%s-%s-%s", imageRepository, pluginVersion, os, arch);
      String imageTar =
          String.format("plugin-%s-%s-%s-%s.tar", plugin.getName(), pluginVersion, os, arch);
      String imagePath = Paths.get(distDir, imageTar).toString();
      logger.info("Building docker image {}", imageTag);
      // GITHUB_ACTOR and GITHUB_TOKEN are required for the Dockerfile to pull the CloudQuery Java
      // libs from GitHub Packages
      String githubActor = System.getenv("GITHUB_ACTOR");
      if (Strings.isNullOrEmpty(githubActor)) {
        logger.error("GITHUB_ACTOR env variable is required");
        throw new IllegalArgumentException("GITHUB_ACTOR env variable is required");
      }
      String githubToken = System.getenv("GITHUB_TOKEN");
      if (Strings.isNullOrEmpty(githubToken)) {
        logger.error("GITHUB_TOKEN env variable is required");
        throw new IllegalArgumentException("GITHUB_TOKEN env variable is required");
      }
      String[] dockerBuildArguments = {
        "buildx",
        "build",
        "-t",
        imageTag,
        "--platform",
        String.format("%s/%s", os, arch),
        "-f",
        dockerFilePath,
        ".",
        "--progress",
        "plain",
        "--load",
        "--build-arg",
        String.format("GITHUB_ACTOR=%s", githubActor),
        "--build-arg",
        String.format("GITHUB_TOKEN=%s", githubToken),
      };
      logger.debug("Running docker command: '{}'", String.join(" ", dockerBuildArguments));
      runCommand(dockerBuildArguments);
      logger.debug("Saving docker image {} to {}", imageTag, imagePath);
      String[] dockerSaveArguments = {"save", "-o", imagePath, imageTag};
      logger.debug("Running docker command: '{}'", String.join(" ", dockerSaveArguments));
      runCommand(dockerSaveArguments);
      try (InputStream is = Files.newInputStream(Paths.get(imagePath))) {
        String checksum = DigestUtils.sha256Hex(is);
        SupportedTargetJson supportedTarget =
            new SupportedTargetJson(os, arch, imageTar, checksum, imageTag);
        supportedTargets.add(supportedTarget);
      }
    }

    return supportedTargets;
  }

  private void runCommand(String[] command) throws IOException, InterruptedException {
    ArrayList<String> allArgs = new ArrayList<>(Arrays.asList(command));
    allArgs.add(0, "docker");
    ProcessBuilder processBuilder =
        new ProcessBuilder(allArgs.toArray(new String[allArgs.size()])).inheritIO();
    processBuilder.directory(new File(pluginDirectory));
    Process process = processBuilder.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      logger.error("Failed to run command: '{}'", String.join(" ", command));
      throw new IOException("Failed to run command");
    }
  }

  private void writePackageJson(List<SupportedTargetJson> supportedTargets) throws IOException {
    String outputPath = Paths.get(distDir, "package.json").toString();
    logger.info("Writing package.json to {}", outputPath);
    PackageJson packageJson =
        new PackageJson(
            plugin.getName(),
            plugin.getTeam(),
            plugin.getKind().toString(),
            pluginVersion,
            message,
            supportedTargets);
    String json = packageJson.toJson();
    FileUtils.writeStringToFile(new File(outputPath), json, "UTF-8");
  }
}
