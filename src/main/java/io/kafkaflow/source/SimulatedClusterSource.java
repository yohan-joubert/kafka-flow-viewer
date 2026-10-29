package io.kafkaflow.source;

import io.kafkaflow.source.ClusterSample.GroupSample;
import io.kafkaflow.source.ClusterSample.MemberSample;
import io.kafkaflow.source.ClusterSample.ProducerSample;
import io.kafkaflow.source.ClusterSample.TopicSample;
import io.kafkaflow.source.ClusterSample.Tp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * A fake e-commerce cluster for trying the UI without Kafka ({@code --demo}). Traffic varies over time, one
 * consumer group is too slow and builds lag, one producer pauses periodically, and the click stream comes from
 * non-idempotent producers so it shows up as "unidentified".
 */
public final class SimulatedClusterSource implements ClusterSource {

    private record Topic(String name, int partitions) {}

    /** A producer writing {@code baseRate} msg/s into each listed topic. */
    private record Producer(long id, String txId, Map<String, Double> baseRate, boolean idempotent, int pauseEverySec) {}

    /** A consumer group able to process {@code capacity} msg/s per topic (null = always keeps up). */
    private record Group(String id, List<String> topics, Double capacity, int members, String clientPrefix) {}

    private final Random random = new Random(42);
    private final long start = System.currentTimeMillis();
    private long last = start;

    private final List<Topic> topics = List.of(
            new Topic("orders", 6),
            new Topic("payments", 3),
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
            new Producer(4040, "order-enricher-0", Map.of("notifications", 55.0, "orders-dlq", 0.4), true, 0),
            new Producer(0, null, Map.of("user-clicks", 850.0), false, 0));

    private final List<Group> groups = List.of(
            new Group("payment-service", List.of("orders"), null, 3, "payment-service"),
            new Group("fulfillment", List.of("orders", "payments"), null, 2, "fulfillment"),
            new Group("order-enricher", List.of("orders"), null, 2, "order-enricher-StreamThread"),
            new Group("shipping-tracker", List.of("shipments"), null, 1, "shipping-tracker"),
            new Group("inventory-sync", List.of("inventory-updates"), 430.0, 4, "inventory-sync"),
            new Group("clickstream-analytics", List.of("user-clicks"), null, 6, "spark-executor"),
            new Group("notifier", List.of("notifications"), null, 2, "notifier"),
            new Group("dlq-inspector", List.of("orders-dlq"), null, 1, "dlq-inspector"),
            new Group("audit-archiver", List.of("audit-log"), null, 0, "audit-archiver"));

    private final Map<Tp, Long> endOffsets = new HashMap<>();
    private final Map<String, Map<Tp, Long>> committed = new HashMap<>();
    private final Map<String, Integer> sequences = new HashMap<>(); // producerId|tp -> last sequence
    private final Map<String, Long> lastWrite = new HashMap<>();
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

        // Produce.
        for (Producer pr : producers) {
            if (pr.pauseEverySec() > 0 && (t % pr.pauseEverySec()) > pr.pauseEverySec() * 0.65) continue;
            for (Map.Entry<String, Double> e : pr.baseRate().entrySet()) {
                double wave = 1 + 0.35 * Math.sin(t / 23.0 + pr.id()) + 0.15 * Math.sin(t / 5.0 + pr.id() * 0.7);
                double count = e.getValue() * wave * dt * (0.9 + 0.2 * random.nextDouble());
                String carryKey = pr.id() + "|" + e.getKey();
                count += carry.getOrDefault(carryKey, 0.0);
                long whole = (long) count;
                carry.put(carryKey, count - whole);
                List<Tp> parts = partitions(e.getKey());
                for (long i = 0; i < whole; ) {
                    Tp tp = parts.get(random.nextInt(parts.size()));
                    long batch = Math.min(whole - i, 1 + random.nextInt(50));
                    endOffsets.merge(tp, batch, Long::sum);
                    if (pr.idempotent()) {
                        String key = pr.id() + "|" + tp.topic() + "|" + tp.partition();
                        sequences.merge(key, (int) batch, Integer::sum);
                        lastWrite.put(key, now);
                    }
                    i += batch;
                }
            }
        }

        // Consume.
        for (Group g : groups) {
            if (g.members() == 0) continue;
            Map<Tp, Long> c = committed.get(g.id());
            for (String topic : g.topics()) {
                List<Tp> parts = partitions(topic);
                double budget = g.capacity() == null ? Double.MAX_VALUE : g.capacity() * dt / parts.size();
                for (Tp tp : parts) {
                    long end = endOffsets.get(tp);
                    long cur = c.getOrDefault(tp, end);
                    // Consumers commit periodically, so they trail the end offset a little even when keeping up.
                    long target = g.capacity() == null ? end - random.nextInt(20) : (long) (cur + budget);
                    c.put(tp, Math.max(cur, Math.min(end, target)));
                }
            }
        }

        return new ClusterSample(now, "demo-cluster-7Hq2", 3, topicSamples(), groupSamples(), producerSamples(now),
                List.of());
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
            List<MemberSample> members = new ArrayList<>();
            for (int m = 0; m < g.members(); m++) {
                Set<String> assigned = new TreeSet<>(g.topics());
                int partitions = 0;
                for (String topic : g.topics()) partitions += partitions(topic).size();
                members.add(new MemberSample("member-" + m, g.clientPrefix() + "-" + (m + 1),
                        "/10.0.3." + (11 + m + g.id().length()), assigned, Math.max(1, partitions / g.members())));
            }
            String state = g.members() == 0 ? "EMPTY" : "STABLE";
            out.add(new GroupSample(g.id(), state, members, new HashMap<>(committed.get(g.id()))));
        }
        return out;
    }

    private List<ProducerSample> producerSamples(long now) {
        List<ProducerSample> out = new ArrayList<>();
        for (Producer pr : producers) {
            if (!pr.idempotent()) continue;
            for (String topic : pr.baseRate().keySet()) {
                for (Tp tp : partitions(topic)) {
                    String key = pr.id() + "|" + tp.topic() + "|" + tp.partition();
                    Long written = lastWrite.get(key);
                    if (written == null || now - written > 60_000) continue;
                    out.add(new ProducerSample(pr.id(), 0, tp, sequences.get(key) - 1, written, pr.txId()));
                }
            }
        }
        return out;
    }

    private List<Tp> partitions(String topic) {
        for (Topic t : topics) {
            if (t.name().equals(topic)) {
                List<Tp> l = new ArrayList<>(t.partitions());
                for (int p = 0; p < t.partitions(); p++) l.add(new Tp(topic, p));
                return l;
            }
        }
        throw new IllegalArgumentException(topic);
    }
}
