package io.kafkaflow.source;

import io.kafkaflow.source.ClusterSample.GroupSample;
import io.kafkaflow.source.ClusterSample.MemberSample;
import io.kafkaflow.source.ClusterSample.ProducerSample;
import io.kafkaflow.source.ClusterSample.TopicSample;
import io.kafkaflow.source.ClusterSample.Tp;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * A fake e-commerce cluster for trying the UI without Kafka ({@code --demo}).
 *
 * <p>Besides plain producers and consumers it has processing chains, where a consumer group writes what it reads
 * to the next topic (consume -> transform -> produce), for example:
 * <pre>payment-processor -> payments -> fraud-check -> payments-verified -> ledger-writer -> ledger-entries -> reporting</pre>
 * Relay producers use a transactional id starting with their group id, as Kafka Streams does with exactly-once,
 * which is how the viewer ties them to their group.
 *
 * <p>Traffic varies over time, inventory-sync is too slow and builds lag, one producer pauses periodically, and the
 * click stream comes from non-idempotent producers so it shows up as "unidentified".
 */
public final class SimulatedClusterSource implements ClusterSource {

    private record Topic(String name, int partitions) {}

    /** A producer writing {@code baseRate} msg/s into each listed topic. */
    private record Producer(long id, String txId, Map<String, Double> baseRate, boolean idempotent, int pauseEverySec) {}

    /**
     * A consumer group able to process {@code capacity} msg/s per topic (null = always keeps up).
     * If {@code outputs} is not empty the group is a processing step: for every message consumed it writes
     * {@code ratio} messages to each output topic, with producer {@code relayId} / {@code relayTxId}.
     */
    private record Group(String id, List<String> topics, Double capacity, int members, String clientPrefix,
                         Map<String, Double> outputs, long relayId, String relayTxId) {

        static Group consumer(String id, List<String> topics, Double capacity, int members, String clientPrefix) {
            return new Group(id, topics, capacity, members, clientPrefix, Map.of(), 0, null);
        }

        static Group relay(String id, List<String> topics, int members, String clientPrefix,
                           Map<String, Double> outputs, long relayId, String relayTxId) {
            return new Group(id, topics, null, members, clientPrefix, outputs, relayId, relayTxId);
        }
    }

    private record ProducerKey(long producerId, Tp tp) {}

    private final Random random = new Random(42);
    private final long start = System.currentTimeMillis();
    private long last = start;

    private final List<Topic> topics = List.of(
            new Topic("orders", 6),
            new Topic("payments", 3),
            new Topic("payments-verified", 3),
            new Topic("ledger-entries", 6),
            new Topic("shipments", 3),
            new Topic("inventory-updates", 12),
            new Topic("user-clicks", 24),
            new Topic("notifications", 3),
            new Topic("orders-dlq", 1),
            new Topic("audit-log", 1));

    private final List<Producer> producers = List.of(
            new Producer(4001, null, Map.of("orders", 110.0), true, 0),
            new Producer(4002, null, Map.of("orders", 95.0), true, 0),
            new Producer(4010, "payment-processor-0", Map.of("payments", 80.0), true, 0),
            new Producer(4020, null, Map.of("shipments", 35.0), true, 90),
            new Producer(4030, null, Map.of("inventory-updates", 260.0), true, 0),
            new Producer(4031, null, Map.of("inventory-updates", 240.0), true, 0),
            new Producer(0, null, Map.of("user-clicks", 850.0), false, 0));

    // Upstream steps before downstream ones, so a message can travel the whole chain within one sample.
    private final List<Group> groups = List.of(
            Group.consumer("payment-service", List.of("orders"), null, 3, "payment-service"),
            Group.consumer("fulfillment", List.of("orders", "payments"), null, 2, "fulfillment"),
            Group.relay("order-enricher", List.of("orders"), 2, "order-enricher-StreamThread",
                    Map.of("notifications", 0.27, "orders-dlq", 0.002), 4040, "order-enricher-7c1e2f-0"),
            Group.relay("fraud-check", List.of("payments"), 2, "fraud-check",
                    Map.of("payments-verified", 0.97), 4050, "fraud-check-0"),
            Group.relay("ledger-writer", List.of("payments-verified"), 2, "ledger-writer",
                    Map.of("ledger-entries", 2.0), 4060, "ledger-writer-0"),
            Group.consumer("reporting", List.of("ledger-entries"), null, 1, "reporting"),
            Group.consumer("shipping-tracker", List.of("shipments"), null, 1, "shipping-tracker"),
            Group.consumer("inventory-sync", List.of("inventory-updates"), 430.0, 4, "inventory-sync"),
            Group.consumer("clickstream-analytics", List.of("user-clicks"), null, 6, "spark-executor"),
            Group.consumer("notifier", List.of("notifications"), null, 2, "notifier"),
            Group.consumer("dlq-inspector", List.of("orders-dlq"), null, 1, "dlq-inspector"),
            Group.consumer("audit-archiver", List.of("audit-log"), null, 0, "audit-archiver"));

    private final Map<Tp, Long> endOffsets = new HashMap<>();
    private final Map<String, Map<Tp, Long>> committed = new HashMap<>();
    private final Map<ProducerKey, Integer> sequences = new HashMap<>();
    private final Map<ProducerKey, Long> lastWrite = new HashMap<>();
    private final Map<Long, String> transactionalIds = new HashMap<>();
    private final Map<String, Double> carry = new HashMap<>();       // fractional messages not yet written

    public SimulatedClusterSource() {
        for (Topic t : topics) {
            for (int p = 0; p < t.partitions(); p++) {
                endOffsets.put(new Tp(t.name(), p), 1_000_000L + random.nextInt(5_000_000));
            }
        }
        for (Group g : groups) {
            Map<Tp, Long> c = new HashMap<>();
            for (String topic : g.topics()) {
                for (Tp tp : partitions(topic)) c.put(tp, endOffsets.get(tp) - random.nextInt(50));
            }
            committed.put(g.id(), c);
        }
        endOffsets.put(new Tp("audit-log", 0), 48_210L);
        committed.get("audit-archiver").put(new Tp("audit-log", 0), 48_210L);
    }

    @Override
    public String describe() {
        return "simulated cluster (demo)";
    }

    @Override
    public synchronized ClusterSample sample() {
        long now = System.currentTimeMillis();
        double dt = (now - last) / 1000.0;
        double t = (now - start) / 1000.0;
        last = now;

        // Producers at the edge of the system.
        for (Producer pr : producers) {
            if (pr.pauseEverySec() > 0 && (t % pr.pauseEverySec()) > pr.pauseEverySec() * 0.65) continue;
            for (Map.Entry<String, Double> e : pr.baseRate().entrySet()) {
                double wave = 1 + 0.35 * Math.sin(t / 23.0 + pr.id()) + 0.15 * Math.sin(t / 5.0 + pr.id() * 0.7);
                double count = e.getValue() * wave * dt * (0.9 + 0.2 * random.nextDouble());
                write(pr.id(), pr.idempotent() ? pr.txId() : null, pr.idempotent(), e.getKey(), count, now);
            }
        }

        // Consumers, then whatever the processing steps write downstream.
        for (Group g : groups) {
            if (g.members() == 0) continue;
            Map<Tp, Long> c = committed.get(g.id());
            long consumed = 0;
            for (String topic : g.topics()) {
                List<Tp> parts = partitions(topic);
                double budget = g.capacity() == null ? Double.MAX_VALUE : g.capacity() * dt / parts.size();
                for (Tp tp : parts) {
                    long end = endOffsets.get(tp);
                    long cur = c.getOrDefault(tp, end);
                    // Consumers commit periodically, so they trail the end offset a little even when keeping up.
                    long target = g.capacity() == null ? end - random.nextInt(20) : (long) (cur + budget);
                    long next = Math.max(cur, Math.min(end, target));
                    consumed += next - cur;
                    c.put(tp, next);
                }
            }
            for (Map.Entry<String, Double> out : g.outputs().entrySet()) {
                write(g.relayId(), g.relayTxId(), true, out.getKey(), consumed * out.getValue(), now);
            }
        }

        return new ClusterSample(now, "demo-cluster-7Hq2", 3, topicSamples(), groupSamples(), producerSamples(now),
                List.of());
    }

    /** Appends {@code count} messages (fractions carried over) to random partitions of a topic. */
    private void write(long producerId, String txId, boolean idempotent, String topic, double count, long now) {
        String carryKey = producerId + "|" + topic;
        count += carry.getOrDefault(carryKey, 0.0);
        long whole = (long) count;
        carry.put(carryKey, count - whole);
        if (txId != null) transactionalIds.put(producerId, txId);
        List<Tp> parts = partitions(topic);
        for (long i = 0; i < whole; ) {
            Tp tp = parts.get(random.nextInt(parts.size()));
            long batch = Math.min(whole - i, 1 + random.nextInt(50));
            endOffsets.merge(tp, batch, Long::sum);
            if (idempotent) {
                ProducerKey key = new ProducerKey(producerId, tp);
                sequences.merge(key, (int) batch, Integer::sum);
                lastWrite.put(key, now);
            }
            i += batch;
        }
    }

    private Map<String, TopicSample> topicSamples() {
        Map<String, TopicSample> out = new LinkedHashMap<>();
        for (Topic t : topics) {
            Map<Integer, Long> offsets = new HashMap<>();
            for (Tp tp : partitions(t.name())) offsets.put(tp.partition(), endOffsets.get(tp));
            out.put(t.name(), new TopicSample(t.name(), t.partitions(), 3, offsets));
        }
        return out;
    }

    private List<GroupSample> groupSamples() {
        List<GroupSample> out = new ArrayList<>();
        for (Group g : groups) {
            // Round-robin assignment, like Kafka's RoundRobinAssignor.
            List<Set<Tp>> assignments = new ArrayList<>();
            for (int m = 0; m < g.members(); m++) assignments.add(new HashSet<>());
            int next = 0;
            for (String topic : g.topics()) {
                for (Tp tp : partitions(topic)) {
                    if (g.members() > 0) assignments.get(next++ % g.members()).add(tp);
                }
            }
            List<MemberSample> members = new ArrayList<>();
            for (int m = 0; m < g.members(); m++) {
                Set<String> assignedTopics = new TreeSet<>();
                for (Tp tp : assignments.get(m)) assignedTopics.add(tp.topic());
                members.add(new MemberSample("member-" + m, g.clientPrefix() + "-" + (m + 1),
                        "/10.0.3." + (11 + m + g.id().length()), assignedTopics, assignments.get(m).size(),
                        assignments.get(m)));
            }
            String state = g.members() == 0 ? "EMPTY" : "STABLE";
            out.add(new GroupSample(g.id(), state, members, new HashMap<>(committed.get(g.id()))));
        }
        return out;
    }

    private List<ProducerSample> producerSamples(long now) {
        List<ProducerSample> out = new ArrayList<>();
        for (Map.Entry<ProducerKey, Integer> e : sequences.entrySet()) {
            ProducerKey key = e.getKey();
            Long written = lastWrite.get(key);
            if (written == null || now - written > 60_000) continue;
            out.add(new ProducerSample(key.producerId(), 0, key.tp(), e.getValue() - 1, written,
                    transactionalIds.get(key.producerId())));
        }
        return out;
    }

    // ---- sample message ----------------------------------------------------------------------

    /**
     * Builds a plausible message for the topic. The content is derived from (topic, partition, offset), so the
     * same offset always shows the same message. Retention is pretended to keep the last ~40% of each partition.
     */
    @Override
    public synchronized SampledMessage sampleMessage(String topic, int maxBytes) {
        List<Tp> parts = partitions(topic);
        Tp tp = parts.get(random.nextInt(parts.size()));
        long last = endOffsets.get(tp) - 1;
        long first = (long) (last * 0.6);
        if (last < first) return null;
        long offset = first + (long) (random.nextDouble() * (last - first + 1));
        Random r = new Random((topic + "/" + tp.partition() + "/" + offset).hashCode());
        long ts = System.currentTimeMillis() - (long) ((last - offset) * 1000.0 / 40) - r.nextInt(1000);

        String id = Long.toString(100_000 + Math.abs(r.nextLong() % 900_000));
        String sku = "SKU-" + (1000 + r.nextInt(9000));
        String customer = "cus_" + Long.toString(Math.abs(r.nextLong()), 36).substring(0, 8);
        String amount = String.format(java.util.Locale.ROOT, "%.2f", 5 + r.nextDouble() * 400);
        String time = java.time.Instant.ofEpochMilli(ts).toString();
        List<SampledMessage.Header> headers = new ArrayList<>();
        headers.add(header("traceparent", "00-" + hex(r, 16) + "-" + hex(r, 8) + "-01", maxBytes));
        String key = "order-" + id;
        byte[] value;
        switch (topic) {
            case "orders" -> value = json("""
                    {"orderId":"%s","customerId":"%s","createdAt":"%s","currency":"EUR","total":%s,                    "items":[{"sku":"%s","quantity":%d,"unitPrice":%s}],"channel":"%s"}""",
                    id, customer, time, amount, sku, 1 + r.nextInt(4), amount, r.nextBoolean() ? "web" : "ios");
            case "payments" -> value = json("""
                    {"paymentId":"pay_%s","orderId":"%s","amount":%s,"currency":"EUR","method":"%s","status":"AUTHORIZED"}""",
                    hex(r, 6), id, amount, r.nextBoolean() ? "card" : "paypal");
            case "payments-verified" -> value = json("""
                    {"paymentId":"pay_%s","orderId":"%s","riskScore":%.3f,"decision":"ACCEPT","checkedBy":"fraud-check"}""",
                    hex(r, 6), id, r.nextDouble() * 0.3);
            case "ledger-entries" -> value = json("""
                    {"entryId":"le_%s","orderId":"%s","account":"%s","direction":"%s","amount":%s,"currency":"EUR"}""",
                    hex(r, 6), id, r.nextBoolean() ? "receivables" : "revenue", r.nextBoolean() ? "DEBIT" : "CREDIT", amount);
            case "shipments" -> value = json("""
                    {"shipmentId":"shp_%s","orderId":"%s","carrier":"%s","status":"%s","eta":"%s"}""",
                    hex(r, 6), id, r.nextBoolean() ? "colissimo" : "dhl", r.nextBoolean() ? "LABEL_CREATED" : "IN_TRANSIT",
                    java.time.Instant.ofEpochMilli(ts + 86_400_000L * (1 + r.nextInt(4))).toString().substring(0, 10));
            case "inventory-updates" -> {
                key = sku;
                value = json("""
                        {"sku":"%s","warehouse":"%s","delta":%d,"onHand":%d,"at":"%s"}""",
                        sku, r.nextBoolean() ? "PAR-1" : "LYS-2", -(1 + r.nextInt(5)), r.nextInt(800), time);
            }
            case "user-clicks" -> {
                // Avro in the Confluent wire format, decoded with an in-memory "registry" (schema id 42).
                key = null;
                GenericRecord click = new GenericData.Record(CLICK);
                click.put("sessionId", "s_" + hex(r, 6));
                click.put("userId", r.nextInt(3) == 0 ? null : customer);
                click.put("page", r.nextBoolean() ? "/checkout" : "/products/" + sku);
                click.put("element", r.nextBoolean() ? "add-to-cart" : "buy-now");
                click.put("at", ts);
                click.put("durationMs", 40 + r.nextInt(2000));
                value = avro(CLICK_SCHEMA_ID, click);
                headers.clear();
            }
            case "notifications" -> value = json("""
                    {"to":"%s","template":"order-confirmation","orderId":"%s","locale":"fr-FR"}""", customer, id);
            case "orders-dlq" -> {
                value = json("""
                        {"orderId":"%s","customerId":"%s","total":"%s EUR"}""", id, customer, amount);
                headers.add(header("error.class", "com.fasterxml.jackson.databind.exc.InvalidFormatException", maxBytes));
                headers.add(header("error.message", "Cannot deserialize value of type `java.math.BigDecimal` from String \"" + amount + " EUR\"", maxBytes));
                headers.add(header("source.topic", "orders", maxBytes));
            }
            default -> value = json("""
                    {"event":"%s","actor":"%s","at":"%s"}""", topic, customer, time);
        }
        headers.add(header("content-type", topic.equals("user-clicks") ? "application/vnd.kafka.avro.v2" : "application/json", maxBytes));
        return new SampledMessage(topic, tp.partition(), offset, ts, "CreateTime",
                key == null ? SampledMessage.Payload.NULL : SampledMessage.Payload.of(key.getBytes(java.nio.charset.StandardCharsets.UTF_8), maxBytes),
                DEMO_REGISTRY.decode(value, maxBytes), headers, first, last);
    }

    private static final int CLICK_SCHEMA_ID = 42;
    private static final Schema CLICK = new Schema.Parser().parse("""
            {"type": "record", "name": "UserClick", "namespace": "io.kafkaflow.demo", "fields": [
              {"name": "sessionId", "type": "string"},
              {"name": "userId", "type": ["null", "string"], "default": null},
              {"name": "page", "type": "string"},
              {"name": "element", "type": "string"},
              {"name": "at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
              {"name": "durationMs", "type": "int"}
            ]}""");
    private static final AvroDecoder DEMO_REGISTRY = new AvroDecoder(id -> {
        if (id != CLICK_SCHEMA_ID) throw new RestClientException("Schema " + id + " not found", 404, 40403);
        return new AvroSchema(CLICK);
    });

    /** Confluent wire format: magic byte 0, 4-byte schema id, Avro binary body. */
    private static byte[] avro(int schemaId, GenericRecord record) {
        try {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            out.write(0);
            out.write(java.nio.ByteBuffer.allocate(4).putInt(schemaId).array());
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static byte[] json(String format, Object... args) {
        return String.format(java.util.Locale.ROOT, format, args).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static SampledMessage.Header header(String key, String value, int maxBytes) {
        return new SampledMessage.Header(key,
                SampledMessage.Payload.of(value.getBytes(java.nio.charset.StandardCharsets.UTF_8), Math.min(maxBytes, 2048)));
    }

    private static String hex(Random r, int bytes) {
        byte[] b = new byte[bytes];
        r.nextBytes(b);
        return java.util.HexFormat.of().formatHex(b);
    }

    private List<Tp> partitions(String topic) {
        for (Topic t : topics) {
            if (t.name().equals(topic)) {
                List<Tp> l = new ArrayList<>(t.partitions());
                for (int p = 0; p < t.partitions(); p++) l.add(new Tp(topic, p));
                return l;
            }
        }
        throw new IllegalArgumentException("No topic named " + topic);
    }
}
