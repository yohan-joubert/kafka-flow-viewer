package io.kafkaflow.config;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Application settings.
 *
 * <p>Sources, lowest to highest priority:
 * <ol>
 *   <li>built-in defaults ({@code default.properties}),</li>
 *   <li>a properties file ({@code --config path}, or {@code ./kafka-flow.properties} if present),</li>
 *   <li>command-line flags (see {@link Cli}).</li>
 * </ol>
 *
 * <p>Every key starting with {@code kafka.} is passed (prefix removed) to the Kafka AdminClient, so any client
 * setting works: {@code kafka.security.protocol=SASL_SSL}, {@code kafka.sasl.jaas.config=...}, etc.
 * Likewise every {@code registry.*} key but {@code registry.url} goes to the Schema Registry client.
 */
public final class AppConfig {

    public static final String KAFKA_PREFIX = "kafka.";
    public static final String REGISTRY_PREFIX = "registry.";
    private static final Path DEFAULT_FILE = Path.of("kafka-flow.properties");

    /** Command-line flags. Each one sets a property, so it overrides the same key from the file. */
    @Command(name = "java -jar kafka-flow-viewer.jar", sortOptions = false,
            description = "Kafka Flow Viewer - live producers -> topics -> consumer groups view",
            footer = {"", "Examples:",
                    "  java -jar kafka-flow-viewer.jar --bootstrap localhost:9092",
                    "  java -jar kafka-flow-viewer.jar --config my-cluster.properties",
                    "  java -jar kafka-flow-viewer.jar --demo     (no Kafka needed)"})
    static final class Cli {
        @Option(names = {"-b", "--bootstrap"}, paramLabel = "<hosts>", description = "Kafka bootstrap servers")
        String bootstrap;
        @Option(names = "--config", paramLabel = "<file>",
                description = "properties file (defaults to ./kafka-flow.properties if it exists)")
        Path config;
        @Option(names = "--registry", paramLabel = "<url>", description = "Schema Registry URL, to decode Avro sample messages")
        String registry;
        @Option(names = {"-p", "--port"}, paramLabel = "<port>", description = "HTTP port for the UI (default 8080)")
        Integer port;
        @Option(names = "--interval", paramLabel = "<ms>", description = "polling interval (default 2000)")
        Long interval;
        @Option(names = "--set", paramLabel = "key=value",
                description = "set any property from default.properties, or any kafka.* / registry.* client setting")
        Map<String, String> set = new LinkedHashMap<>();
        @Option(names = "--demo", description = "run against a simulated cluster")
        boolean demo;
        @Option(names = {"-h", "--help"}, usageHelp = true, description = "show this help")
        boolean help;

        void applyTo(Properties p) {
            set.forEach((k, v) -> p.setProperty(k.trim(), v.trim()));
            if (bootstrap != null) p.setProperty("kafka.bootstrap.servers", bootstrap);
            if (registry != null) p.setProperty("registry.url", registry);
            if (port != null) p.setProperty("http.port", port.toString());
            if (interval != null) p.setProperty("poll.interval.ms", interval.toString());
            if (demo) p.setProperty("demo", "true");
        }
    }

    private final Properties props;
    private final Path configFile;
    private final boolean help;

    private AppConfig(Properties props, Path configFile, boolean help) {
        this.props = props;
        this.configFile = configFile;
        this.help = help;
    }

    /** Parses the command line. Bad flags or an unreadable config file throw IllegalArgumentException. */
    public static AppConfig fromArgs(String[] args) {
        Cli cli = new Cli();
        CommandLine cmd = new CommandLine(cli);
        try {
            cmd.parseArgs(args);
        } catch (CommandLine.ParameterException e) {
            throw new IllegalArgumentException(e.getMessage() + " (try --help)", e);
        }
        Properties p = defaults();
        Path file = cli.config != null ? cli.config : Files.exists(DEFAULT_FILE) ? DEFAULT_FILE : null;
        if (file != null) {
            if (!Files.isRegularFile(file)) throw new IllegalArgumentException("Config file not found: " + file);
            try (Reader r = Files.newBufferedReader(file)) {
                p.load(r);
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot read config file " + file + ": " + e.getMessage(), e);
            }
        }
        cli.applyTo(p);
        return new AppConfig(p, file, cmd.isUsageHelpRequested());
    }

    public static AppConfig of(Properties overrides) {
        Properties p = defaults();
        p.putAll(overrides);
        return new AppConfig(p, null, false);
    }

    private static Properties defaults() {
        Properties p = new Properties();
        try (InputStream in = AppConfig.class.getResourceAsStream("/default.properties")) {
            p.load(in);
        } catch (IOException | NullPointerException e) {
            throw new IllegalStateException("Cannot read default.properties", e);
        }
        return p;
    }

    public static String usage() {
        return new CommandLine(new Cli()).getUsageMessage();
    }

    // ---- typed accessors -------------------------------------------------------------------

    public boolean help() { return help; }
    public boolean demo() { return bool("demo"); }
    public int httpPort() { return Integer.parseInt(get("http.port")); }
    public String httpHost() { return get("http.host"); }
    public Duration pollInterval() { return millis("poll.interval.ms"); }
    public Duration adminTimeout() { return millis("admin.timeout.ms"); }
    public boolean includeInternalTopics() { return bool("topics.internal"); }
    public Pattern topicInclude() { return pattern("topics.include"); }
    public Pattern topicExclude() { return pattern("topics.exclude"); }
    public Pattern groupInclude() { return pattern("groups.include"); }
    public Pattern groupExclude() { return pattern("groups.exclude"); }
    public boolean producersEnabled() { return bool("producers.enabled"); }
    public Duration producerIdleTimeout() { return millis("producers.idle.timeout.ms"); }
    public boolean sampleEnabled() { return bool("sample.enabled"); }
    public int sampleMaxBytes() { return Integer.parseInt(get("sample.max.bytes")); }
    public double rateSmoothing() { return Double.parseDouble(get("rate.smoothing")); }
    public String configFile() { return configFile == null ? null : configFile.toAbsolutePath().toString(); }

    /** AdminClient / client properties: every {@code kafka.*} key with the prefix removed. */
    public Properties kafkaProperties() {
        Properties k = new Properties();
        section(KAFKA_PREFIX).forEach(k::setProperty);
        return k;
    }

    /** Schema Registry URL(s), comma separated, or null when no registry is configured. */
    public String registryUrl() {
        String v = props.getProperty(REGISTRY_PREFIX + "url", "").trim();
        return v.isEmpty() ? null : v;
    }

    /**
     * Schema Registry client settings: every {@code registry.*} key but the URL, with the prefix removed.
     * Giving {@code registry.basic.auth.user.info} is enough for basic auth: the credentials source then defaults
     * to {@code USER_INFO}.
     */
    public Map<String, Object> registryConfig() {
        Map<String, Object> c = new TreeMap<>();
        section(REGISTRY_PREFIX).forEach((key, value) -> {
            // Values are passed as given (a password may end with a space); only durations are trimmed.
            if (!key.equals("url")) c.put(key, key.endsWith(".ms") || key.endsWith(".sec") ? value.trim() : value);
        });
        if (c.containsKey("basic.auth.user.info")) c.putIfAbsent("basic.auth.credentials.source", "USER_INFO");
        return c;
    }

    public String bootstrapServers() {
        return props.getProperty("kafka.bootstrap.servers", "");
    }

    /** Every key starting with {@code prefix}, with the prefix removed. */
    private Map<String, String> section(String prefix) {
        Map<String, String> out = new TreeMap<>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(prefix)) out.put(name.substring(prefix.length()), props.getProperty(name));
        }
        return out;
    }

    /** A setting as trimmed text: values in properties files may carry trailing spaces. */
    private String get(String key) {
        String v = props.getProperty(key);
        if (v == null) throw new IllegalStateException("Missing setting: " + key);
        return v.trim();
    }

    private Duration millis(String key) {
        return Duration.ofMillis(Long.parseLong(get(key)));
    }

    private boolean bool(String key) {
        return Boolean.parseBoolean(props.getProperty(key, "false").trim());
    }

    private Pattern pattern(String key) {
        String v = props.getProperty(key, "").trim();
        return v.isEmpty() ? null : Pattern.compile(v);
    }
}
