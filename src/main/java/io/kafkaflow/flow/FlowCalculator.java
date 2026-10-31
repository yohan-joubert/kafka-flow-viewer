package io.kafkaflow.flow;

import io.kafkaflow.flow.FlowSnapshot.GroupView;
import io.kafkaflow.flow.FlowSnapshot.Link;
import io.kafkaflow.flow.FlowSnapshot.MemberView;
import io.kafkaflow.flow.FlowSnapshot.PartitionRate;
import io.kafkaflow.flow.FlowSnapshot.ProducerView;
import io.kafkaflow.flow.FlowSnapshot.TopicView;
import io.kafkaflow.flow.FlowSnapshot.Totals;
import io.kafkaflow.source.ClusterSample;
import io.kafkaflow.source.ClusterSample.GroupSample;
import io.kafkaflow.source.ClusterSample.MemberSample;
import io.kafkaflow.source.ClusterSample.ProducerSample;
import io.kafkaflow.source.ClusterSample.TopicSample;
import io.kafkaflow.source.ClusterSample.Tp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Turns consecutive {@link ClusterSample}s into a {@link FlowSnapshot}: every rate is
 * (counter now - counter before) / elapsed time, then smoothed with an exponential moving average.
 *
 * <ul>
 *   <li>topic in-rate: growth of partition end offsets</li>
 *   <li>group consume rate (per topic): growth of committed offsets</li>
 *   <li>producer rate (per topic): growth of the producer's last sequence number</li>
 * </ul>
 * Not thread-safe; called from the single polling thread.
 */
public final class FlowCalculator {

    static final String UNIDENTIFIED = "p:unidentified";

    private final double smoothing;
    private final Map<String, Double> smoothed = new HashMap<>();
    private ClusterSample previous;
    private Map<String, List<PartitionRate>> partitions = Map.of();

    public FlowCalculator(double smoothing) {
        if (smoothing < 0 || smoothing >= 1) throw new IllegalArgumentException("rate.smoothing must be in [0, 1)");
        this.smoothing = smoothing;
    }

    public FlowSnapshot update(ClusterSample now, String sourceName, long pollIntervalMs) {
        ClusterSample before = previous;
        // Rates are meaningless across two different clusters (e.g. after a config change).
        if (before != null && before.clusterId() != null && !before.clusterId().equals(now.clusterId())) before = null;
        double dt = before == null ? 0 : Math.max(0.001, (now.timestampMs() - before.timestampMs()) / 1000.0);
        Set<String> seenKeys = new HashSet<>();

        // ---- topics: end-offset growth -------------------------------------------------------
        Map<String, Double> topicIn = new HashMap<>();
        Map<String, Long> topicMessages = new HashMap<>();
        for (TopicSample t : now.topics().values()) {
            long total = 0;
            for (long o : t.endOffsets().values()) total += o;
            topicMessages.put(t.name(), total);
            long delta = 0;
            TopicSample prev = before == null ? null : before.topics().get(t.name());
            if (prev != null) {
                for (Map.Entry<Integer, Long> e : t.endOffsets().entrySet()) {
                    Long p = prev.endOffsets().get(e.getKey());
                    if (p != null && e.getValue() >= p) delta += e.getValue() - p;
                }
            }
            topicIn.put(t.name(), rate("topic:" + t.name(), delta, dt, before != null, seenKeys));
        }

        // ---- consumer groups: committed-offset growth and lag ---------------------------------
        Map<String, GroupSample> prevGroups = new HashMap<>();
        if (before != null) for (GroupSample g : before.groups()) prevGroups.put(g.groupId(), g);

        List<GroupView> groups = new ArrayList<>();
        List<Link> links = new ArrayList<>();
        Map<String, Double> topicOut = new HashMap<>();
        Map<String, Long> topicLag = new HashMap<>();
        Map<String, Integer> topicGroupCount = new HashMap<>();

        for (GroupSample g : now.groups()) {
            GroupSample prev = prevGroups.get(g.groupId());
            Map<String, Long> deltaByTopic = new TreeMap<>();
            Map<String, Long> lagByTopic = new TreeMap<>();
            for (Map.Entry<Tp, Long> e : g.committedOffsets().entrySet()) {
                Tp tp = e.getKey();
                TopicSample topic = now.topics().get(tp.topic());
                if (topic == null) continue;
                deltaByTopic.merge(tp.topic(), 0L, Long::sum);
                Long end = topic.endOffsets().get(tp.partition());
                if (end != null) lagByTopic.merge(tp.topic(), Math.max(0, end - e.getValue()), Long::sum);
                Long p = prev == null ? null : prev.committedOffsets().get(tp);
                if (p != null && e.getValue() >= p) deltaByTopic.merge(tp.topic(), e.getValue() - p, Long::sum);
            }
            // Topics assigned to members but with nothing committed yet still get a (zero-rate) link.
            for (MemberSample m : g.members()) {
                for (String t : m.topics()) if (now.topics().containsKey(t)) deltaByTopic.putIfAbsent(t, 0L);
            }

            String gid = "g:" + g.groupId();
            double groupRate = 0;
            long groupLag = 0;
            for (Map.Entry<String, Long> e : deltaByTopic.entrySet()) {
                String topic = e.getKey();
                double r = rate("consume:" + g.groupId() + "\u0000" + topic, e.getValue(), dt, prev != null, seenKeys);
                long lag = lagByTopic.getOrDefault(topic, 0L);
                groupRate += r;
                groupLag += lag;
                topicOut.merge(topic, r, Double::sum);
                topicLag.merge(topic, lag, Long::sum);
                topicGroupCount.merge(topic, 1, Integer::sum);
                links.add(new Link(gid + "<" + topic, "t:" + topic, gid, "consume", r, lag));
            }

            List<MemberView> members = new ArrayList<>();
            for (MemberSample m : g.members()) {
                members.add(new MemberView(m.clientId(), m.host(), List.copyOf(m.topics()), m.partitions()));
            }
            members.sort(Comparator.comparing(MemberView::clientId, Comparator.nullsLast(Comparator.naturalOrder())));
            groups.add(new GroupView(gid, g.groupId(), g.state(), members, groupRate, groupLag,
                    List.copyOf(deltaByTopic.keySet()), 0, List.of(), List.of()));
        }

        // ---- producers: sequence-number growth --------------------------------------------------
        Map<String, ProducerSample> prevProducers = new HashMap<>();
        if (before != null) for (ProducerSample p : before.producers()) prevProducers.put(producerKey(p), p);

        Map<Long, ProducerAgg> aggs = new TreeMap<>();
        for (ProducerSample p : now.producers()) {
            ProducerAgg agg = aggs.computeIfAbsent(p.producerId(), ProducerAgg::new);
            ProducerSample prev = prevProducers.get(producerKey(p));
            agg.deltaByTopic.merge(p.partition().topic(), prev == null ? 0 : sequenceDelta(prev, p), Long::sum);
            if (agg.txId == null) agg.txId = p.transactionalId();
            agg.lastSeen = Math.max(agg.lastSeen, p.lastTimestampMs());
        }

        List<ProducerView> producers = new ArrayList<>();
        Map<String, Double> attributedIn = new HashMap<>();
        Map<String, Integer> topicProducerCount = new HashMap<>();
        for (ProducerAgg agg : aggs.values()) {
            String pid = "p:" + agg.producerId;
            double total = 0;
            for (Map.Entry<String, Long> e : agg.deltaByTopic.entrySet()) {
                String topic = e.getKey();
                double r = rate("produce:" + agg.producerId + "\u0000" + topic, e.getValue(), dt, before != null, seenKeys);
                total += r;
                attributedIn.merge(topic, r, Double::sum);
                topicProducerCount.merge(topic, 1, Integer::sum);
                links.add(new Link(pid + ">" + topic, pid, "t:" + topic, "produce", r, 0));
            }
            String label = agg.txId != null ? agg.txId : "producer " + agg.producerId;
            producers.add(new ProducerView(pid, label, agg.producerId, agg.txId, false, total, agg.lastSeen,
                    List.copyOf(agg.deltaByTopic.keySet())));
        }

        // Traffic that no identified producer explains (non-idempotent producers, old brokers, ACLs...)
        // goes to one synthetic node so that every active topic visibly has an inflow.
        List<String> residualTopics = new ArrayList<>();
        double residualTotal = 0;
        for (Map.Entry<String, Double> e : topicIn.entrySet()) {
            double residual = e.getValue() - attributedIn.getOrDefault(e.getKey(), 0.0);
            if (residual > Math.max(0.5, 0.05 * e.getValue())) {
                residualTopics.add(e.getKey());
                residualTotal += residual;
                links.add(new Link(UNIDENTIFIED + ">" + e.getKey(), UNIDENTIFIED, "t:" + e.getKey(), "produce", residual, 0));
            }
        }
        if (!residualTopics.isEmpty()) {
            residualTopics.sort(null);
            producers.add(new ProducerView(UNIDENTIFIED, "Unidentified producers", null, null, true, residualTotal,
                    now.timestampMs(), residualTopics));
        }

        // ---- chains: groups that also write (consume -> transform -> produce) --------------------
        // Kafka has no link between a consumer group and a producer, except by naming: Kafka Streams with
        // exactly-once uses group.id = application.id and transactional.id = "<application.id>-...".
        // Any producer whose transactional id is the group id, or starts with "<group id>-", is folded into
        // that group, so the group becomes a processing step: topic -> group -> topic.
        Map<String, List<ProducerView>> relays = new TreeMap<>();
        List<ProducerView> standalone = new ArrayList<>();
        List<String> groupIds = new ArrayList<>();
        for (GroupView g : groups) groupIds.add(g.groupId());
        for (ProducerView p : producers) {
            String owner = p.synthetic() ? null : owningGroup(p.transactionalId(), groupIds);
            if (owner == null) standalone.add(p);
            else relays.computeIfAbsent(owner, k -> new ArrayList<>()).add(p);
        }
        if (!relays.isEmpty()) {
            Map<String, String> newSource = new HashMap<>();
            for (Map.Entry<String, List<ProducerView>> e : relays.entrySet()) {
                for (ProducerView p : e.getValue()) newSource.put(p.id(), "g:" + e.getKey());
            }
            Map<String, Link> merged = new LinkedHashMap<>();
            for (Link l : links) {
                String src = newSource.get(l.source());
                if (src == null) {
                    merged.put(l.id(), l);
                    continue;
                }
                String id = src + ">" + l.target().substring(2);
                Link prev = merged.get(id);
                merged.put(id, new Link(id, src, l.target(), "produce", l.rate() + (prev == null ? 0 : prev.rate()), 0));
            }
            links = new ArrayList<>(merged.values());

            List<GroupView> withRelays = new ArrayList<>(groups.size());
            for (GroupView g : groups) {
                List<ProducerView> ps = relays.get(g.groupId());
                if (ps == null) {
                    withRelays.add(g);
                    continue;
                }
                double out = 0;
                Set<String> outTopics = new TreeSet<>();
                List<Long> ids = new ArrayList<>();
                for (ProducerView p : ps) {
                    out += p.rate();
                    outTopics.addAll(p.topics());
                    ids.add(p.producerId());
                }
                withRelays.add(new GroupView(g.id(), g.groupId(), g.state(), g.members(), g.rate(), g.lag(), g.topics(),
                        out, List.copyOf(outTopics), ids));
            }
            groups = withRelays;
            producers = standalone;
        }

        // ---- topics view + totals -----------------------------------------------------------
        List<TopicView> topics = new ArrayList<>();
        double totalIn = 0, totalOut = 0;
        long totalLag = 0, totalMessages = 0;
        for (TopicSample t : now.topics().values()) {
            double in = topicIn.getOrDefault(t.name(), 0.0);
            double out = topicOut.getOrDefault(t.name(), 0.0);
            long lag = topicLag.getOrDefault(t.name(), 0L);
            long messages = topicMessages.getOrDefault(t.name(), 0L);
            totalIn += in;
            totalOut += out;
            totalLag += lag;
            totalMessages += messages;
            topics.add(new TopicView("t:" + t.name(), t.name(), t.partitions(), t.replicationFactor(), messages, in, out,
                    lag, topicProducerCount.getOrDefault(t.name(), 0), topicGroupCount.getOrDefault(t.name(), 0)));
        }
        topics.sort(Comparator.comparing(TopicView::name));
        groups.sort(Comparator.comparing(GroupView::groupId));

        Map<Long, String> producerOwner = new HashMap<>();
        for (Map.Entry<String, List<ProducerView>> e : relays.entrySet()) {
            for (ProducerView p : e.getValue()) producerOwner.put(p.producerId(), "g:" + e.getKey());
        }
        partitions = partitionRates(now, before, dt, prevGroups, prevProducers, producerOwner, seenKeys);

        smoothed.keySet().retainAll(seenKeys);
        previous = now;

        return new FlowSnapshot(now.timestampMs(), sourceName, now.clusterId(), now.brokerCount(), pollIntervalMs,
                before == null,
                new Totals(totalIn, totalOut, totalLag, topics.size(), groups.size(), producers.size(), totalMessages),
                topics, groups, producers, links, now.warnings(), null);
    }

    /** Per-partition numbers from the last {@link #update}, keyed by node id (t:..., g:..., p:...). */
    public Map<String, List<PartitionRate>> partitions() {
        return partitions;
    }

    private Map<String, List<PartitionRate>> partitionRates(ClusterSample now, ClusterSample before, double dt,
                                                            Map<String, GroupSample> prevGroups,
                                                            Map<String, ProducerSample> prevProducers,
                                                            Map<Long, String> producerOwner, Set<String> seenKeys) {
        boolean baseline = before != null;
        Map<String, List<PartitionRate>> out = new HashMap<>();

        // Topic partitions: write rate.
        Map<Tp, Double> written = new HashMap<>();
        for (TopicSample t : now.topics().values()) {
            TopicSample prev = baseline ? before.topics().get(t.name()) : null;
            for (Map.Entry<Integer, Long> e : t.endOffsets().entrySet()) {
                Tp tp = new Tp(t.name(), e.getKey());
                Long p = prev == null ? null : prev.endOffsets().get(e.getKey());
                long delta = p != null && e.getValue() >= p ? e.getValue() - p : 0;
                written.put(tp, rate("pt:" + t.name() + "\u0000" + tp.partition(), delta, dt, prev != null, seenKeys));
            }
        }

        // Consumer groups: consume rate, lag, owning member.
        Map<Tp, Double> consumed = new HashMap<>();
        Map<Tp, Long> lagged = new HashMap<>();
        for (GroupSample g : now.groups()) {
            GroupSample prev = prevGroups.get(g.groupId());
            Map<Tp, String> owner = new HashMap<>();
            for (MemberSample m : g.members()) {
                if (m.assignment() != null) for (Tp tp : m.assignment()) owner.put(tp, m.clientId());
            }
            List<PartitionRate> rows = new ArrayList<>();
            for (Map.Entry<Tp, Long> e : g.committedOffsets().entrySet()) {
                Tp tp = e.getKey();
                TopicSample topic = now.topics().get(tp.topic());
                if (topic == null) continue;
                Long end = topic.endOffsets().get(tp.partition());
                long lag = end == null ? 0 : Math.max(0, end - e.getValue());
                Long p = prev == null ? null : prev.committedOffsets().get(tp);
                long delta = p != null && e.getValue() >= p ? e.getValue() - p : 0;
                double r = rate("pc:" + g.groupId() + "\u0000" + tp.topic() + "\u0000" + tp.partition(), delta, dt,
                        prev != null, seenKeys);
                consumed.merge(tp, r, Double::sum);
                lagged.merge(tp, lag, Long::sum);
                rows.add(new PartitionRate(tp.topic(), tp.partition(), "read", r, 0, lag, e.getValue(), owner.get(tp)));
            }
            // Assigned partitions with no commit yet.
            for (Map.Entry<Tp, String> e : owner.entrySet()) {
                if (!g.committedOffsets().containsKey(e.getKey()) && now.topics().containsKey(e.getKey().topic())) {
                    rows.add(new PartitionRate(e.getKey().topic(), e.getKey().partition(), "read", 0, 0, 0, -1, e.getValue()));
                }
            }
            out.put("g:" + g.groupId(), rows);
        }

        for (TopicSample t : now.topics().values()) {
            List<PartitionRate> rows = new ArrayList<>();
            for (Map.Entry<Integer, Long> e : t.endOffsets().entrySet()) {
                Tp tp = new Tp(t.name(), e.getKey());
                rows.add(new PartitionRate(t.name(), e.getKey(), "topic", written.getOrDefault(tp, 0.0),
                        consumed.getOrDefault(tp, 0.0), lagged.getOrDefault(tp, 0L), e.getValue(), null));
            }
            out.put("t:" + t.name(), rows);
        }

        // Producers: sequence growth per partition. Relay producers report under their group.
        Map<Tp, Double> attributed = new HashMap<>();
        for (ProducerSample p : now.producers()) {
            ProducerSample prev = prevProducers.get(producerKey(p));
            long delta = prev == null ? 0 : sequenceDelta(prev, p);
            double r = rate("pp:" + producerKey(p), delta, dt, baseline, seenKeys);
            attributed.merge(p.partition(), r, Double::sum);
            String node = producerOwner.getOrDefault(p.producerId(), "p:" + p.producerId());
            out.computeIfAbsent(node, k -> new ArrayList<>()).add(new PartitionRate(p.partition().topic(),
                    p.partition().partition(), "write", r, 0, 0, p.lastSequence(), null));
        }

        // Whatever no identified producer explains.
        List<PartitionRate> residual = new ArrayList<>();
        for (Map.Entry<Tp, Double> e : written.entrySet()) {
            double r = e.getValue() - attributed.getOrDefault(e.getKey(), 0.0);
            if (r > Math.max(0.05, 0.05 * e.getValue())) {
                residual.add(new PartitionRate(e.getKey().topic(), e.getKey().partition(), "write", r, 0, 0, -1, null));
            }
        }
        if (!residual.isEmpty()) out.put(UNIDENTIFIED, residual);

        Comparator<PartitionRate> order = Comparator.comparing(PartitionRate::role)
                .thenComparing(PartitionRate::topic).thenComparingInt(PartitionRate::partition);
        Map<String, List<PartitionRate>> sorted = new HashMap<>();
        for (Map.Entry<String, List<PartitionRate>> e : out.entrySet()) {
            List<PartitionRate> rows = new ArrayList<>(e.getValue());
            rows.sort(order);
            sorted.put(e.getKey(), List.copyOf(rows));
        }
        return Map.copyOf(sorted);
    }

    /** The group a transactional producer belongs to by naming convention (longest matching group id), or null. */
    static String owningGroup(String transactionalId, List<String> groupIds) {
        if (transactionalId == null) return null;
        String best = null;
        for (String g : groupIds) {
            boolean match = transactionalId.equals(g) || transactionalId.startsWith(g + "-");
            if (match && (best == null || g.length() > best.length())) best = g;
        }
        return best;
    }

    /** Records written between two observations of the same producer on the same partition. */
    static long sequenceDelta(ProducerSample before, ProducerSample now) {
        if (now.epoch() != before.epoch()) return now.lastSequence() + 1L; // epoch bump restarts sequences at 0
        long d = (long) now.lastSequence() - before.lastSequence();
        if (d >= 0) return d;
        return (Integer.MAX_VALUE - (long) before.lastSequence()) + now.lastSequence() + 1L; // sequence wrapped
    }

    private static String producerKey(ProducerSample p) {
        return p.producerId() + "\u0000" + p.partition().topic() + "\u0000" + p.partition().partition();
    }

    private double rate(String key, long delta, double dt, boolean hasBaseline, Set<String> seen) {
        seen.add(key);
        if (!hasBaseline || dt <= 0) return 0;
        double raw = delta / dt;
        Double prev = smoothed.get(key);
        double value = prev == null ? raw : smoothing * prev + (1 - smoothing) * raw;
        if (value < 0.005) value = 0; // avoid "0.00001 msg/s" tails from the moving average
        smoothed.put(key, value);
        return value;
    }

    /** For tests: forget everything. */
    void reset() {
        previous = null;
        smoothed.clear();
    }

    private static final class ProducerAgg {
        final long producerId;
        final Map<String, Long> deltaByTopic = new TreeMap<>();
        String txId;
        long lastSeen;

        ProducerAgg(long producerId) {
            this.producerId = producerId;
        }
    }
}
