package io.kafkaflow.flow;

import java.util.List;

/**
 * What the UI draws: nodes (producers, topics, consumer groups) and links between them, with rates in messages/s.
 * Serialized to JSON as-is.
 *
 * <p>Node ids are prefixed by kind: {@code p:<producerId>}, {@code t:<topic>}, {@code g:<groupId>}.
 */
public record FlowSnapshot(
        long timestamp,
        String source,
        String clusterId,
        int brokers,
        long pollIntervalMs,
        boolean warmingUp,
        Totals totals,
        List<TopicView> topics,
        List<GroupView> groups,
        List<ProducerView> producers,
        List<Link> links,
        List<String> warnings,
        String error
) {

    public record Totals(double inRate, double outRate, long lag, int topics, int groups, int producers, long messages) {}

    public record TopicView(
            String id,
            String name,
            int partitions,
            int replicationFactor,
            long messages,
            double inRate,
            double outRate,
            long lag,
            int producerCount,
            int groupCount
    ) {}

    public record GroupView(
            String id,
            String groupId,
            String state,
            List<MemberView> members,
            double rate,
            long lag,
            List<String> topics,
            // Set when the group also writes (consume -> transform -> produce), see FlowCalculator.
            double produceRate,
            List<String> outputTopics,
            List<Long> producerIds
    ) {}

    public record MemberView(String clientId, String host, List<String> topics, int partitions) {}

    public record ProducerView(
            String id,
            String label,
            Long producerId,          // null for the synthetic "unidentified producers" node
            String transactionalId,
            boolean synthetic,
            double rate,
            long lastSeenMs,
            List<String> topics
    ) {}

    /** kind is "produce" (producer -> topic) or "consume" (topic -> group). lag is only set for consume links. */
    public record Link(String id, String source, String target, String kind, double rate, long lag) {}

    public static FlowSnapshot error(String source, String message, FlowSnapshot last, long pollIntervalMs) {
        if (last == null) {
            return new FlowSnapshot(System.currentTimeMillis(), source, null, 0, pollIntervalMs, true,
                    new Totals(0, 0, 0, 0, 0, 0, 0), List.of(), List.of(), List.of(), List.of(), List.of(), message);
        }
        return new FlowSnapshot(System.currentTimeMillis(), last.source(), last.clusterId(), last.brokers(),
                last.pollIntervalMs(), last.warmingUp(), last.totals(), last.topics(), last.groups(), last.producers(),
                last.links(), last.warnings(), message);
    }
}
