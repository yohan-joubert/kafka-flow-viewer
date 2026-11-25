package io.kafkaflow.config;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppConfigTest {

    private static AppConfig config(String... keyValues) {
        Properties p = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) p.setProperty(keyValues[i], keyValues[i + 1]);
        return AppConfig.of(p);
    }

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
