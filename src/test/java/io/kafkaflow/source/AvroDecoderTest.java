package io.kafkaflow.source;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.kafkaflow.source.SampledMessage.Payload;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroDecoderTest {

    // Exact BigDecimals: the default node factory would turn 1234.50 into 1234.5 and hide a lost scale.
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    private static final Schema ORDER = new Schema.Parser().parse("""
            {"type": "record", "name": "Order", "namespace": "shop", "fields": [
              {"name": "id", "type": "long"},
              {"name": "customer", "type": {"type": "record", "name": "Customer", "fields": [
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": null}]}},
              {"name": "note", "type": ["null", "string"], "default": null},
              {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "PAID"]}},
              {"name": "tags", "type": {"type": "array", "items": "string"}},
              {"name": "attributes", "type": {"type": "map", "values": "int"}},
              {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
              {"name": "createdAt", "type": {"type": "long", "logicalType": "timestamp-millis"}},
              {"name": "day", "type": {"type": "int", "logicalType": "date"}},
              {"name": "ref", "type": {"type": "string", "logicalType": "uuid"}},
              {"name": "ratio", "type": "double"},
              {"name": "score", "type": "double"},
              {"name": "signature", "type": "bytes"}
            ]}""");

    private static final long BIG_ID = 9_007_199_254_740_993L; // 2^53 + 1: a JavaScript number would round it

    private final MockSchemaRegistryClient registry = new MockSchemaRegistryClient();
    private final AvroDecoder decoder = new AvroDecoder(registry::getSchemaById);

    // ---- decoding ----------------------------------------------------------------------------

    @Test
    void decodesARecordToIndentedJson() throws Exception {
        int id = register("orders-value", ORDER);
        Payload p = decoder.decode(orderBytes(id), 65536);

        assertEquals("avro", p.encoding());
        assertEquals(new SampledMessage.SchemaInfo(id, "shop.Order"), p.schema());
        assertNull(p.problem());
        assertFalse(p.truncated());
        assertEquals(orderBytes(id).length, p.size());
        assertTrue(p.text().startsWith("{\n"), "indented JSON expected, got: " + p.text());

        JsonNode n = JSON.readTree(p.text());
        assertEquals("Ada", n.at("/customer/name").asText());
        assertEquals("ada@example.com", n.at("/customer/email").asText(), "a union value has no {\"string\": …} wrapper");
        assertTrue(n.get("note").isNull());
        assertEquals("PAID", n.get("status").asText());
        assertEquals(List.of("gift", "express"), List.of(n.at("/tags/0").asText(), n.at("/tags/1").asText()));
        assertEquals(3, n.at("/attributes/quantity").asInt());
    }

    @Test
    void keepsNumbersExact() throws Exception {
        JsonNode n = JSON.readTree(decoder.decode(orderBytes(register("orders-value", ORDER)), 65536).text());
        assertTrue(n.get("id").isIntegralNumber());
        assertEquals(BIG_ID, n.get("id").longValue());
        assertEquals(new BigDecimal("0.1234567890123"), n.get("ratio").decimalValue());
        assertEquals("NaN", n.get("score").asText(), "NaN is not valid JSON, so it is written as a string");
    }

    @Test
    void showsLogicalTypesReadably() throws Exception {
        JsonNode n = JSON.readTree(decoder.decode(orderBytes(register("orders-value", ORDER)), 65536).text());
        assertEquals(new BigDecimal("1234.50"), n.get("amount").decimalValue());
        assertEquals("2026-11-25T10:15:30.123Z", n.get("createdAt").asText());
        assertEquals("2026-11-25", n.get("day").asText());
        assertEquals("3f2a1c9e-5b7d-4e8f-9a0b-1c2d3e4f5a6b", n.get("ref").asText());
        assertEquals("cafe01", n.get("signature").asText(), "plain bytes are shown as hex");
    }

    @Test
    void decodesAPrimitiveKey() throws Exception {
        Schema string = Schema.create(Schema.Type.STRING);
        int id = register("orders-key", string);
        Payload p = decoder.decode(wire(id, string, "order-42"), 4096);
        assertEquals("avro", p.encoding());
        assertEquals("order-42", p.text(), "a top-level string is shown without JSON quotes");
        assertEquals(new SampledMessage.SchemaInfo(id, "string"), p.schema());

        Schema longSchema = Schema.create(Schema.Type.LONG);
        int longId = register("ids-key", longSchema);
        assertEquals(String.valueOf(BIG_ID), decoder.decode(wire(longId, longSchema, BIG_ID), 4096).text());
    }

    @Test
    void cutsLongJsonWithoutBreakingCharacters() throws Exception {
        Schema string = Schema.create(Schema.Type.STRING);
        int id = register("notes-value", string);
        byte[] bytes = wire(id, string, "é".repeat(100)); // 2 bytes each in UTF-8
        Payload p = decoder.decode(bytes, 51);
        assertEquals("avro", p.encoding());
        assertTrue(p.truncated());
        assertEquals("é".repeat(25), p.text());
        assertEquals(bytes.length, p.size());
    }

    @Test
    void looksUpEachSchemaOnce() throws Exception {
        int id = register("orders-value", ORDER);
        AtomicInteger calls = new AtomicInteger();
        AvroDecoder counting = new AvroDecoder(i -> {
            calls.incrementAndGet();
            return registry.getSchemaById(i);
        });
        counting.decode(orderBytes(id), 65536);
        counting.decode(orderBytes(id), 65536);
        assertEquals(1, calls.get());
    }

    // ---- not Avro: unchanged behaviour -------------------------------------------------------

    @Test
    void leavesOtherFormatsAlone() {
        assertEquals(Payload.NULL, decoder.decode(null, 100));
        Payload text = decoder.decode("{\"a\":1}".getBytes(StandardCharsets.UTF_8), 100);
        assertEquals("text", text.encoding());
        assertNull(text.problem());
    }

    @Test
    void schemaIdZeroIsNotAvro() {
        // A long key serialized by LongSerializer also starts with 0x00: schema id 0 does not exist in a registry.
        AvroDecoder neverCalled = new AvroDecoder(i -> { throw new AssertionError("no lookup expected"); });
        Payload p = neverCalled.decode(new byte[] {0, 0, 0, 0, 0, 0, 0, 42}, 100);
        assertEquals("hex", p.encoding());
        assertNull(p.problem());
    }

    // ---- failures: hex plus an explanation ---------------------------------------------------

    @Test
    void withoutRegistryAvroStaysHexWithAHint() throws Exception {
        byte[] bytes = orderBytes(42);
        Payload p = AvroDecoder.disabled().decode(bytes, 65536);
        assertEquals("hex", p.encoding());
        assertEquals(Payload.of(bytes, 65536).text(), p.text());
        assertNotNull(p.problem());
        assertTrue(p.problem().contains("42"), p.problem());
        assertTrue(p.problem().contains("registry.url"), p.problem());
    }

    @Test
    void unknownSchemaIdIsExplained() throws Exception {
        Payload p = decoder.decode(orderBytes(777), 65536);
        assertEquals("hex", p.encoding());
        assertTrue(p.problem().contains("777"), p.problem());
        assertTrue(p.problem().toLowerCase().contains("not found"), p.problem());
    }

    @Test
    void registryErrorsAreExplained() throws Exception {
        AvroDecoder down = new AvroDecoder(i -> { throw new IOException("Connection refused"); });
        Payload p = down.decode(orderBytes(5), 65536);
        assertEquals("hex", p.encoding());
        assertTrue(p.problem().contains("Connection refused"), p.problem());

        AvroDecoder forbidden = new AvroDecoder(i -> { throw new RestClientException("Unauthorized", 401, 40101); });
        Payload q = forbidden.decode(orderBytes(5), 65536);
        assertEquals("hex", q.encoding());
        assertTrue(q.problem().toLowerCase().contains("not allowed"), q.problem());
    }

    @Test
    void nonAvroSchemaIsExplained() throws Exception {
        AvroSchema protobuf = new AvroSchema("\"string\"") {
            @Override
            public String schemaType() {
                return "PROTOBUF";
            }
        };
        Payload p = new AvroDecoder(i -> protobuf).decode(orderBytes(9), 65536);
        assertEquals("hex", p.encoding());
        assertTrue(p.problem().contains("PROTOBUF"), p.problem());
    }

    @Test
    void bytesThatDoNotMatchTheSchemaAreExplained() throws Exception {
        int id = register("orders-value", ORDER);
        byte[] good = orderBytes(id);

        byte[] extra = java.util.Arrays.copyOf(good, good.length + 3); // trailing garbage
        Payload p = decoder.decode(extra, 65536);
        assertEquals("hex", p.encoding());
        assertNotNull(p.problem());

        byte[] cut = java.util.Arrays.copyOf(good, good.length - 4); // body ends too early
        Payload q = decoder.decode(cut, 65536);
        assertEquals("hex", q.encoding());
        assertNotNull(q.problem());
    }

    // ---- review fixes -------------------------------------------------------------------------

    @Test
    void logicalValuesOutOfRangeFallBackToTheRawValue() throws Exception {
        Schema odd = new Schema.Parser().parse("""
                {"type": "record", "name": "Odd", "fields": [
                  {"name": "early", "type": {"type": "int", "logicalType": "time-millis"}},
                  {"name": "late", "type": {"type": "int", "logicalType": "time-millis"}},
                  {"name": "huge", "type": {"type": "long", "logicalType": "time-micros"}},
                  {"name": "empty", "type": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}}
                ]}""");
        GenericRecord r = new GenericData.Record(odd);
        r.put("early", -1);
        r.put("late", 90_000_000); // 25 h
        r.put("huge", Long.MAX_VALUE);
        r.put("empty", ByteBuffer.wrap(new byte[0]));
        Payload p = decoder.decode(wire(register("odd-value", odd), odd, r), 65536);

        assertEquals("avro", p.encoding(), String.valueOf(p.problem()));
        JsonNode n = JSON.readTree(p.text());
        assertEquals(-1, n.get("early").asInt());
        assertEquals(90_000_000, n.get("late").asInt());
        assertEquals(Long.MAX_VALUE, n.get("huge").longValue());
        assertEquals("", n.get("empty").asText());
    }

    @Test
    void missingSchemasAreNotLookedUpOnEverySample() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        long[] now = {0};
        AvroDecoder d = new AvroDecoder(i -> {
            calls.incrementAndGet();
            throw new RestClientException("Schema " + i + " not found", 404, 40403);
        }, () -> now[0]);
        byte[] longKey = ByteBuffer.allocate(8).putLong(1_000_000_000_000L).array(); // 00 00 00 e8 ...: "id" 232
        Payload first = d.decode(longKey, 100);
        Payload second = d.decode(longKey, 100);
        assertEquals(1, calls.get());
        assertEquals(first.problem(), second.problem());
        assertEquals("hex", second.encoding());

        now[0] += 10 * 60_000; // much later the id is tried again: it may have been registered since
        d.decode(longKey, 100);
        assertEquals(2, calls.get());
    }

    @Test
    void anUnreachableRegistryIsNotRetriedForEveryPayload() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        long[] now = {0};
        AvroDecoder d = new AvroDecoder(i -> {
            calls.incrementAndGet();
            throw new IOException("Connection refused");
        }, () -> now[0]);
        Payload key = d.decode(orderBytes(5), 65536);
        Payload value = d.decode(orderBytes(6), 65536); // another id: still no second call while the registry is down
        assertEquals(1, calls.get());
        assertTrue(key.problem().contains("Connection refused"), key.problem());
        assertTrue(value.problem().contains("Connection refused"), value.problem());

        now[0] += 5 * 60_000;
        d.decode(orderBytes(6), 65536);
        assertEquals(2, calls.get());
    }

    @Test
    void aProtobufSchemaFromARealRegistryIsExplained() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer fake = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fake.createContext("/schemas/ids/9", ex -> {
            requests.incrementAndGet();
            byte[] body = """
                    {"schemaType": "PROTOBUF", "schema": "syntax = \\"proto3\\"; message Click { string page = 1; }"}"""
                    .getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/vnd.schemaregistry.v1+json");
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        });
        fake.start();
        try (AvroDecoder real = AvroDecoder.forRegistry("http://127.0.0.1:" + fake.getAddress().getPort(), Map.of())) {
            byte[] protobuf = {0, 0, 0, 0, 9, 0, 10, 4, 'h', 'o', 'm', 'e'};
            Payload p = real.decode(protobuf, 1000);
            assertEquals("hex", p.encoding());
            assertTrue(p.problem().contains("PROTOBUF"), p.problem());
            assertTrue(p.problem().contains("Only Avro is decoded"), p.problem());
            int afterFirst = requests.get();
            real.decode(protobuf, 1000);
            assertEquals(afterFirst, requests.get(), "a non-Avro schema is remembered");
        } finally {
            fake.stop(0);
        }
    }

    @Test
    void badRegistrySettingsDoNotStopTheViewer() {
        AvroDecoder d = AvroDecoder.forRegistry("https://127.0.0.1:1",
                Map.of("schema.registry.ssl.truststore.location", "/does/not/exist.jks",
                        "schema.registry.ssl.truststore.password", "changeit"));
        Payload p = d.decode(new byte[] {0, 0, 0, 0, 5, 2}, 100);
        assertEquals("hex", p.encoding());
        assertTrue(p.problem().toLowerCase().contains("registry settings"), p.problem());
        d.close();
    }

    @Test
    void hugeCollectionsAreNotRenderedPastTheLimit() throws Exception {
        Schema nulls = new Schema.Parser().parse("""
                {"type": "record", "name": "Nulls", "fields": [{"name": "items", "type": {"type": "array", "items": "null"}}]}""");
        int id = register("nulls-value", nulls);
        // One block of 5,000,000 nulls costs 5 bytes on the wire: count (zigzag varint), then the end marker.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(new byte[] {0, 0, 0, (byte) (id >> 8), (byte) id});
        BinaryEncoder e = EncoderFactory.get().binaryEncoder(out, null);
        e.writeLong(5_000_000);
        e.writeLong(0);
        e.flush();
        byte[] bytes = out.toByteArray();

        Payload p = assertTimeoutPreemptively(Duration.ofSeconds(2), () -> decoder.decode(bytes, 1000));
        assertEquals("avro", p.encoding());
        assertTrue(p.truncated());
        assertTrue(p.text().getBytes(StandardCharsets.UTF_8).length <= 1000);

        GenericRecord datum = new GenericData.Record(nulls);
        datum.put("items", java.util.Collections.nCopies(5_000_000, null));
        AvroDecoder.Rendered r = AvroDecoder.render(datum, nulls, 1000);
        assertTrue(r.truncated());
        assertTrue(r.charsProduced() <= 64 * 1024,
                "rendering stops near the limit instead of building the whole JSON: " + r.charsProduced());
    }

    // ---- helpers ------------------------------------------------------------------------------

    private int register(String subject, Schema schema) throws Exception {
        return registry.register(subject, new AvroSchema(schema));
    }

    private static byte[] orderBytes(int schemaId) throws IOException {
        GenericRecord customer = new GenericData.Record(ORDER.getField("customer").schema());
        customer.put("name", "Ada");
        customer.put("email", "ada@example.com");
        Map<String, Integer> attributes = new LinkedHashMap<>();
        attributes.put("quantity", 3);

        GenericRecord order = new GenericData.Record(ORDER);
        order.put("id", BIG_ID);
        order.put("customer", customer);
        order.put("note", null);
        order.put("status", new GenericData.EnumSymbol(ORDER.getField("status").schema(), "PAID"));
        order.put("tags", List.of("gift", "express"));
        order.put("attributes", attributes);
        order.put("amount", ByteBuffer.wrap(new BigDecimal("1234.50").unscaledValue().toByteArray()));
        order.put("createdAt", Instant.parse("2026-11-25T10:15:30.123Z").toEpochMilli());
        order.put("day", (int) LocalDate.parse("2026-11-25").toEpochDay());
        order.put("ref", "3f2a1c9e-5b7d-4e8f-9a0b-1c2d3e4f5a6b");
        order.put("ratio", 0.1234567890123);
        order.put("score", Double.NaN);
        order.put("signature", ByteBuffer.wrap(new byte[] {(byte) 0xca, (byte) 0xfe, 0x01}));
        return wire(schemaId, ORDER, order);
    }

    /** Confluent wire format: magic byte 0, schema id (4 bytes, big-endian), Avro binary body. */
    private static byte[] wire(int schemaId, Schema schema, Object datum) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0);
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<>(schema).write(datum, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    @Test
    void wireHelperWritesTheConfluentHeader() throws IOException {
        byte[] b = wire(258, Schema.create(Schema.Type.INT), 1);
        assertArrayEquals(new byte[] {0, 0, 0, 1, 2, 2}, b);
    }
}
