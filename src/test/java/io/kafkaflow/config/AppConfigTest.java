package io.kafkaflow.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppConfigTest {

    private static AppConfig config(String... keyValues) {
        Properties p = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) p.setProperty(keyValues[i], keyValues[i + 1]);
        return AppConfig.of(p);
    }

    // ---- loading: defaults < properties file < command line ----------------------------------

    @Test
    void defaultsGiveTypedValues() {
        AppConfig c = config();
        assertEquals(8080, c.httpPort());
        assertEquals("0.0.0.0", c.httpHost());
        assertEquals(Duration.ofSeconds(2), c.pollInterval());
        assertEquals(Duration.ofSeconds(10), c.adminTimeout());
        assertEquals(0.5, c.rateSmoothing());
        assertTrue(c.producersEnabled());
        assertEquals(Duration.ofMinutes(1), c.producerIdleTimeout());
        assertTrue(c.sampleEnabled());
        assertEquals(65536, c.sampleMaxBytes());
        assertFalse(c.includeInternalTopics());
        assertNull(c.topicInclude());
        assertNull(c.groupExclude());
        assertFalse(c.demo());
        assertFalse(c.help());
        assertNull(c.configFile());
        assertEquals("localhost:9092", c.bootstrapServers());
    }

    @Test
    void flagsOverrideTheFileWhichOverridesDefaults(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cluster.properties");
        Files.writeString(file, """
                http.port=9000
                poll.interval.ms=5000
                topics.exclude=.*-changelog
                kafka.bootstrap.servers=file-broker:9092
                kafka.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="viewer" password="a=b;c";
                """);
        AppConfig c = AppConfig.fromArgs(new String[] {"--config", file.toString(), "-p", "9100"});

        assertEquals(9100, c.httpPort());                       // flag
        assertEquals(Duration.ofSeconds(5), c.pollInterval());  // file
        assertEquals(Duration.ofSeconds(10), c.adminTimeout()); // default
        assertEquals(".*-changelog", c.topicExclude().pattern());
        assertEquals("file-broker:9092", c.bootstrapServers());
        assertEquals(file.toAbsolutePath().toString(), c.configFile());
        Properties kafka = c.kafkaProperties();
        assertEquals("file-broker:9092", kafka.getProperty("bootstrap.servers"));
        assertEquals("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"viewer\" password=\"a=b;c\";",
                kafka.getProperty("sasl.jaas.config"));
        assertEquals("kafka-flow-viewer", kafka.getProperty("client.id"));
        assertFalse(kafka.containsKey("http.port"));
    }

    @Test
    void shortAndLongFlags() throws Exception {
        AppConfig c = AppConfig.fromArgs(new String[] {
                "-b", "b1:9092,b2:9092", "--interval", "3000", "--demo", "--registry", "http://sr:8081"});
        assertEquals("b1:9092,b2:9092", c.bootstrapServers());
        assertEquals(Duration.ofSeconds(3), c.pollInterval());
        assertTrue(c.demo());
        assertEquals("http://sr:8081", c.registryUrl());

        AppConfig l = AppConfig.fromArgs(new String[] {"--bootstrap", "b3:9092", "--port", "9200"});
        assertEquals("b3:9092", l.bootstrapServers());
        assertEquals(9200, l.httpPort());
    }

    @Test
    void setAcceptsAnyKeyAndKeepsEqualsSignsInTheValue() throws Exception {
        AppConfig c = AppConfig.fromArgs(new String[] {
                "--set", "kafka.security.protocol=SASL_SSL",
                "--set", "topics.include=orders.*",
                "--set", "registry.basic.auth.user.info=key:se=cret"});
        assertEquals("SASL_SSL", c.kafkaProperties().getProperty("security.protocol"));
        assertEquals("orders.*", c.topicInclude().pattern());
        assertEquals("key:se=cret", c.registryConfig().get("basic.auth.user.info"));
    }

    @Test
    void helpFlags() throws Exception {
        assertTrue(AppConfig.fromArgs(new String[] {"-h"}).help());
        assertTrue(AppConfig.fromArgs(new String[] {"--help"}).help());
        String usage = AppConfig.usage();
        for (String flag : List.of("--bootstrap", "-b", "--config", "--registry", "--port", "-p", "--interval", "--set", "--demo")) {
            assertTrue(usage.contains(flag), "usage lists " + flag + ":\n" + usage);
        }
    }

    @Test
    void badArgumentsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> AppConfig.fromArgs(new String[] {"--nope"}));
        assertThrows(IllegalArgumentException.class, () -> AppConfig.fromArgs(new String[] {"-b"}));
        assertThrows(IllegalArgumentException.class, () -> AppConfig.fromArgs(new String[] {"--set", "novalue"}));
    }


    @Test
    void aKeyThatPrefixesAnotherIsKept(@TempDir Path dir) throws Exception {
        // Both are real Schema Registry client settings: one key being a prefix of another must not lose either.
        Path file = dir.resolve("oauth.properties");
        Files.writeString(file, """
                registry.bearer.auth.scope=schema:read
                registry.bearer.auth.scope.claim.name=scp
                kafka.a.b=1
                kafka.a.b.c=2
                """);
        AppConfig c = AppConfig.fromArgs(new String[] {"--config", file.toString()});
        assertEquals("schema:read", c.registryConfig().get("bearer.auth.scope"));
        assertEquals("scp", c.registryConfig().get("bearer.auth.scope.claim.name"));
        assertEquals("1", c.kafkaProperties().getProperty("a.b"));
        assertEquals("2", c.kafkaProperties().getProperty("a.b.c"));
    }

    @Test
    void aMissingConfigFileIsExplained(@TempDir Path dir) {
        Path absent = dir.resolve("absent.properties");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> AppConfig.fromArgs(new String[] {"--config", absent.toString()}));
        assertEquals("Config file not found: " + absent, e.getMessage());
    }

    // ---- registry ------------------------------------------------------------------------------

    @Test
    void noRegistryByDefault() {
        assertNull(config().registryUrl());
        assertNull(config("registry.url", "  ").registryUrl());
    }

    @Test
    void registryFlagSetsTheUrl() throws Exception {
        AppConfig c = AppConfig.fromArgs(new String[] {"--registry", "http://localhost:8081"});
        assertEquals("http://localhost:8081", c.registryUrl());
    }

    @Test
    void registryKeysGoToTheClientWithoutPrefix() {
        AppConfig c = config(
                "registry.url", "https://sr1:8081,https://sr2:8081",
                "registry.basic.auth.user.info", "viewer:secret",
                "registry.schema.registry.ssl.truststore.location", "/etc/truststore.jks",
                "kafka.bootstrap.servers", "broker:9092");
        Map<String, Object> client = c.registryConfig();

        assertEquals("https://sr1:8081,https://sr2:8081", c.registryUrl());
        assertEquals("viewer:secret", client.get("basic.auth.user.info"));
        assertEquals("/etc/truststore.jks", client.get("schema.registry.ssl.truststore.location"));
        assertFalse(client.containsKey("url"));
        assertFalse(client.containsKey("bootstrap.servers"));
    }

    @Test
    void userInfoImpliesUserInfoCredentialsSource() {
        assertEquals("USER_INFO", config("registry.url", "http://sr:8081", "registry.basic.auth.user.info", "u:p")
                .registryConfig().get("basic.auth.credentials.source"));
        assertEquals("URL", config("registry.url", "http://sr:8081", "registry.basic.auth.user.info", "u:p",
                "registry.basic.auth.credentials.source", "URL").registryConfig().get("basic.auth.credentials.source"));
        assertFalse(config("registry.url", "http://sr:8081").registryConfig().containsKey("basic.auth.credentials.source"));
    }

    @Test
    void registrySecretsAreKeptAsGiven() {
        AppConfig c = config("registry.url", " http://sr:8081 ",
                "registry.basic.auth.user.info", "viewer:secret ",
                "registry.schema.registry.ssl.truststore.password", " pass ");
        assertEquals("http://sr:8081", c.registryUrl());
        assertEquals("viewer:secret ", c.registryConfig().get("basic.auth.user.info"));
        assertEquals(" pass ", c.registryConfig().get("schema.registry.ssl.truststore.password"));
    }

    @Test
    void registryTimeoutsHaveShortDefaults() {
        Map<String, Object> client = config("registry.url", "http://sr:8081").registryConfig();
        assertTrue(client.containsKey("http.connect.timeout.ms"));
        assertTrue(client.containsKey("http.read.timeout.ms"));
    }
}
