package io.kafkaflow.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Application settings.
 *
 * <p>Sources, lowest to highest priority:
 * <ol>
 *   <li>built-in defaults,</li>
 *   <li>a properties file ({@code --config path}, or {@code ./kafka-flow.properties} if present),</li>
 *   <li>command-line flags ({@code --bootstrap}, {@code --port}, {@code --demo}, {@code --set key=value}).</li>
 * </ol>
 *
 * <p>Every key starting with {@code kafka.} is passed (prefix removed) to the Kafka AdminClient, so any client
 * setting works: {@code kafka.security.protocol=SASL_SSL}, {@code kafka.sasl.jaas.config=...}, etc.
 */
public final class AppConfig {

    public static final String KAFKA_PREFIX = "kafka.";

    private final Properties props;

    private AppConfig(Properties props) {
        this.props = props;
    }

    public static AppConfig fromArgs(String[] args) throws IOException {
        Properties p = defaults();

        Path configFile = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--config") && i + 1 < args.length) {
                configFile = Path.of(args[i + 1]);
            }
        }
        if (configFile == null && Files.exists(Path.of("kafka-flow.properties"))) {
            configFile = Path.of("kafka-flow.properties");
        }
        if (configFile != null) {
            try (Reader r = Files.newBufferedReader(configFile)) {
                p.load(r);
            }
            p.setProperty("config.file", configFile.toAbsolutePath().toString());
        }

        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            String next = i + 1 < args.length ? args[i + 1] : null;
            switch (a) {
                case "--config" -> i++;
                case "--demo" -> p.setProperty("demo", "true");
                case "--bootstrap", "-b" -> { p.setProperty("kafka.bootstrap.servers", require(a, next)); i++; }
                case "--port", "-p" -> { p.setProperty("http.port", require(a, next)); i++; }
                case "--interval" -> { p.setProperty("poll.interval.ms", require(a, next)); i++; }
                case "--set" -> {
                    String kv = require(a, next);
                    int eq = kv.indexOf('=');
                    if (eq <= 0) throw new IllegalArgumentException("--set expects key=value, got: " + kv);
                    p.setProperty(kv.substring(0, eq).trim(), kv.substring(eq + 1).trim());
                    i++;
                }
                case "--help", "-h" -> p.setProperty("help", "true");
                default -> throw new IllegalArgumentException("Unknown argument: " + a + " (try --help)");
            }
        }
        return new AppConfig(p);
    }

    public static AppConfig of(Properties overrides) {
        Properties p = defaults();
        p.putAll(overrides);
        return new AppConfig(p);
    }

    private static Properties defaults() {
        Properties p = new Properties();
        try (InputStream in = AppConfig.class.getResourceAsStream("/default.properties")) {
            if (in != null) p.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read default.properties", e);
        }
        return p;
    }

    private static String require(String flag, String value) {
        if (value == null || value.startsWith("--")) {
            throw new IllegalArgumentException(flag + " needs a value");
        }
        return value;
    }

    public static String usage() {
        return """
                Kafka Flow Viewer - live producers -> topics -> consumer groups view

                Usage:
                  java -jar kafka-flow-viewer.jar --bootstrap localhost:9092
                  java -jar kafka-flow-viewer.jar --config my-cluster.properties
                  java -jar kafka-flow-viewer.jar --demo            (simulated cluster, no Kafka needed)

                Options:
                  --bootstrap, -b <hosts>   Kafka bootstrap servers
                  --config <file>           properties file (defaults to ./kafka-flow.properties if it exists)
                  --port, -p <port>         HTTP port for the UI (default 8080)
                  --interval <ms>           polling interval (default 2000)
                  --set key=value           set any property, e.g. --set kafka.security.protocol=SSL
                  --demo                    run against a simulated cluster
                """;
    }

    // ---- typed accessors -------------------------------------------------------------------

    public boolean help() { return bool("help"); }
    public boolean demo() { return bool("demo"); }
    public int httpPort() { return Integer.parseInt(get("http.port")); }
    public String httpHost() { return get("http.host"); }
    public Duration pollInterval() { return Duration.ofMillis(Long.parseLong(get("poll.interval.ms"))); }
    public Duration adminTimeout() { return Duration.ofMillis(Long.parseLong(get("admin.timeout.ms"))); }
    public boolean includeInternalTopics() { return bool("topics.internal"); }
    public Pattern topicInclude() { return pattern("topics.include"); }
    public Pattern topicExclude() { return pattern("topics.exclude"); }
    public Pattern groupInclude() { return pattern("groups.include"); }
    public Pattern groupExclude() { return pattern("groups.exclude"); }
    public boolean producersEnabled() { return bool("producers.enabled"); }
    public Duration producerIdleTimeout() { return Duration.ofMillis(Long.parseLong(get("producers.idle.timeout.ms"))); }
    public double rateSmoothing() { return Double.parseDouble(get("rate.smoothing")); }
    public String configFile() { return props.getProperty("config.file"); }

    /** AdminClient / client properties: every {@code kafka.*} key with the prefix removed. */
    public Properties kafkaProperties() {
        Properties k = new Properties();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(KAFKA_PREFIX)) {
                k.setProperty(name.substring(KAFKA_PREFIX.length()), props.getProperty(name));
            }
        }
        return k;
    }

    public String bootstrapServers() {
        return props.getProperty("kafka.bootstrap.servers", "");
    }

    private String get(String key) {
        String v = props.getProperty(key);
        if (v == null) throw new IllegalStateException("Missing setting: " + key);
        return v.trim();
    }

    private boolean bool(String key) {
        return Boolean.parseBoolean(props.getProperty(key, "false").trim());
    }

    private Pattern pattern(String key) {
        String v = props.getProperty(key, "").trim();
        return v.isEmpty() ? null : Pattern.compile(v);
    }
}
