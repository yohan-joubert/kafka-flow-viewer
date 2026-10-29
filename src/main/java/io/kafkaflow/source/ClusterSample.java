package io.kafkaflow.source;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One raw observation of the cluster: absolute counters only (offsets, sequence numbers).
 * Rates are derived later by comparing two consecutive samples.
 */
public record ClusterSample(
        long timestampMs,
        String clusterId,
        int brokerCount,
        Map<String, TopicSample> topics,
        List<GroupSample> groups,
        List<ProducerSample> producers,
        List<String> warnings
) {

    /** Partition identifier, independent of Kafka client classes. */
    public record Tp(String topic, int partition) {}

    public record TopicSample(String name, int partitions, int replicationFactor, Map<Integer, Long> endOffsets) {}

    public record GroupSample(
            String groupId,
            String state,
            List<MemberSample> members,
            Map<Tp, Long> committedOffsets
    ) {}

    public record MemberSample(String memberId, String clientId, String host, Set<String> topics, int partitions) {}

    /**
     * One producer's state on one partition, as returned by describeProducers.
     * {@code lastSequence} increases by one per record written, which gives a per-producer record rate.
     */
    public record ProducerSample(
            long producerId,
            int epoch,
            Tp partition,
            int lastSequence,
            long lastTimestampMs,
            String transactionalId
    ) {}
}
