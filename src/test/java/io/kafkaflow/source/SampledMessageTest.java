package io.kafkaflow.source;

import io.kafkaflow.source.SampledMessage.Payload;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SampledMessageTest {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void textStaysText() {
        Payload p = Payload.of(utf8("{\"orderId\":\"42\",\"note\":\"crème brûlée\"}\n"), 1000);
        assertEquals("text", p.encoding());
        assertEquals("{\"orderId\":\"42\",\"note\":\"crème brûlée\"}\n", p.text());
        assertTrue(!p.truncated());
    }

    @Test
    void binaryBecomesHex() {
        // Confluent wire format: magic byte 0, then a 4-byte schema id.
        Payload p = Payload.of(new byte[] {0, 0, 0, 0, 42, 2, 'a'}, 1000);
        assertEquals("hex", p.encoding());
        assertEquals("00 00 00 00 2a 02 61", p.text());
        assertEquals(7, p.size());

        Payload invalid = Payload.of(new byte[] {'a', (byte) 0xC3, 'b'}, 1000); // broken UTF-8
        assertEquals("hex", invalid.encoding());
    }

    @Test
    void truncationKeepsWholeCharacters() {
        byte[] bytes = utf8("ééééé"); // 2 bytes each
        Payload p = Payload.of(bytes, 5); // cuts the third character in half
        assertEquals("text", p.encoding());
        assertEquals("éé", p.text());
        assertTrue(p.truncated());
        assertEquals(10, p.size());
    }

    @Test
    void nullStaysNull() {
        assertEquals("null", Payload.of(null, 100).encoding());
    }

    @Test
    void simulatorSamplesEveryTopicWithinItsRange() {
        SimulatedClusterSource sim = new SimulatedClusterSource();
        sim.sample();
        for (String topic : sim.sample().topics().keySet()) {
            SampledMessage m = sim.sampleMessage(topic, 65536);
            assertTrue(m.offset() >= m.earliestOffset() && m.offset() <= m.latestOffset());
            assertTrue(m.value().size() > 0);
            if (topic.equals("user-clicks")) assertEquals("avro", m.value().encoding());
            else assertEquals("text", m.value().encoding());
        }
    }

    @Test
    void simulatorShowsDecodedAvro() {
        SimulatedClusterSource sim = new SimulatedClusterSource();
        sim.sample();
        SampledMessage m = sim.sampleMessage("user-clicks", 65536);
        assertEquals("avro", m.value().encoding());
        assertEquals("io.kafkaflow.demo.UserClick", m.value().schema().name());
        assertTrue(m.value().text().contains("\"sessionId\""), m.value().text());
    }
}
