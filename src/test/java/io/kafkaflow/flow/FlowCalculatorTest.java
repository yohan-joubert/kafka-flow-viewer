package io.kafkaflow.flow;

import io.kafkaflow.source.ClusterSample;
import io.kafkaflow.source.ClusterSample.GroupSample;
import io.kafkaflow.source.ClusterSample.MemberSample;
import io.kafkaflow.source.ClusterSample.ProducerSample;
import io.kafkaflow.source.ClusterSample.TopicSample;
import io.kafkaflow.source.ClusterSample.Tp;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlowCalculatorTest {

    private static ClusterSample sample(long t, long end0, long end1, long committed0, long committed1, int seq, int epoch) {
        Tp p0 = new Tp("orders", 0), p1 = new Tp("orders", 1);
        return new ClusterSample(t, "c1", 1,
                Map.of("orders", new TopicSample("orders", 2, 1, Map.of(0, end0, 1, end1))),
                List.of(new GroupSample("billing", "STABLE",
                        List.of(new MemberSample("m1", "billing-1", "/10.0.0.1", Set.of("orders"), 2, Set.of(p0, p1))),
                        Map.of(p0, committed0, p1, committed1))),
                List.of(new ProducerSample(7, epoch, p0, seq, t, null)),
                List.of());
    }

    @Test
    void computesRatesAndLagFromCounterDeltas() {
        FlowCalculator calc = new FlowCalculator(0);
        FlowSnapshot first = calc.update(sample(0, 100, 100, 90, 100, 9, 0), "test", 1000);
        assertTrue(first.warmingUp());
        assertEquals(0, first.totals().inRate());

        // 2 s later: 400 new messages, 200 consumed, producer 7 wrote 100 of them on partition 0.
        FlowSnapshot s = calc.update(sample(2000, 300, 300, 190, 200, 109, 0), "test", 1000);
        assertEquals(200.0, s.topics().get(0).inRate(), 1e-9);
        assertEquals(100.0, s.groups().get(0).rate(), 1e-9);
        assertEquals(110 + 100, s.groups().get(0).lag());
        assertEquals(50.0, s.producers().get(0).rate(), 1e-9);

        // The other 150 msg/s come from producers Kafka could not identify.
        FlowSnapshot.ProducerView unidentified = s.producers().get(1);
        assertTrue(unidentified.synthetic());
        assertEquals(150.0, unidentified.rate(), 1e-9);
        assertEquals(3, s.links().size()); // producer 7, unidentified, billing
    }

    @Test
    void partitionRatesShowSkewAndOwnership() {
        FlowCalculator calc = new FlowCalculator(0);
        calc.update(sample(0, 100, 100, 90, 100, 9, 0), "test", 1000);
        // Partition 0 gets 300 messages, partition 1 gets 100; only partition 0 is consumed.
        calc.update(sample(2000, 400, 200, 290, 100, 109, 0), "test", 1000);

        List<FlowSnapshot.PartitionRate> topic = calc.partitions().get("t:orders");
        assertEquals(2, topic.size());
        assertEquals(150.0, topic.get(0).rate(), 1e-9);
        assertEquals(50.0, topic.get(1).rate(), 1e-9);
        assertEquals(100.0, topic.get(0).outRate(), 1e-9);
        assertEquals(110, topic.get(0).lag());
        assertEquals(100, topic.get(1).lag());

        List<FlowSnapshot.PartitionRate> group = calc.partitions().get("g:billing");
        assertEquals("billing-1", group.get(0).member());
        assertEquals(100.0, group.get(0).rate(), 1e-9);
        assertEquals(0.0, group.get(1).rate(), 1e-9);

        List<FlowSnapshot.PartitionRate> producer = calc.partitions().get("p:7");
        assertEquals(1, producer.size());
        assertEquals(50.0, producer.get(0).rate(), 1e-9);

        // Unattributed traffic per partition: 150 - 50 on p0, 50 on p1.
        List<FlowSnapshot.PartitionRate> residual = calc.partitions().get(FlowCalculator.UNIDENTIFIED);
        assertEquals(2, residual.size());
        assertEquals(100.0, residual.get(0).rate(), 1e-9);
        assertEquals(50.0, residual.get(1).rate(), 1e-9);
    }

    @Test
    void sequenceDeltaHandlesWrapAndEpochBump() {
        Tp tp = new Tp("t", 0);
        ProducerSample before = new ProducerSample(1, 0, tp, Integer.MAX_VALUE - 1, 0, null);
        assertEquals(3, FlowCalculator.sequenceDelta(before, new ProducerSample(1, 0, tp, 1, 0, null)));
        assertEquals(5, FlowCalculator.sequenceDelta(before, new ProducerSample(1, 1, tp, 4, 0, null)));
    }

    @Test
    void offsetResetDoesNotProduceNegativeRates() {
        FlowCalculator calc = new FlowCalculator(0);
        calc.update(sample(0, 1000, 1000, 900, 900, 0, 0), "test", 1000);
        FlowSnapshot s = calc.update(sample(1000, 10, 10, 5, 5, 0, 0), "test", 1000); // topic recreated
        assertEquals(0, s.topics().get(0).inRate());
        assertEquals(0, s.groups().get(0).rate());
    }

    @Test
    void groupThatWritesBecomesAProcessingStepInTheChain() {
        // p1 -> in -> relay (group, transactional.id "relay-0") -> out -> sink
        FlowCalculator calc = new FlowCalculator(0);
        calc.update(chainSample(0, 0, 0, 0, 0), "test", 1000);
        FlowSnapshot s = calc.update(chainSample(1000, 100, 100, 100, 100), "test", 1000);

        assertEquals(1, s.producers().size()); // only p1; the relay producer is folded into its group
        FlowSnapshot.GroupView relay = s.groups().get(0);
        assertEquals("relay", relay.groupId());
        assertEquals(100.0, relay.produceRate(), 1e-9);
        assertEquals("out", relay.outputTopics().get(0));
        assertTrue(s.links().stream().anyMatch(l ->
                l.source().equals("g:relay") && l.target().equals("t:out") && l.kind().equals("produce")));
        assertTrue(s.links().stream().anyMatch(l -> l.source().equals("t:out") && l.target().equals("g:sink")));
    }

    @Test
    void owningGroupUsesLongestPrefix() {
        List<String> groups = List.of("app", "app-orders", "other");
        assertEquals("app-orders", FlowCalculator.owningGroup("app-orders-3f2a-1", groups));
        assertEquals("app", FlowCalculator.owningGroup("app-9c1d-0", groups));
        assertEquals("app", FlowCalculator.owningGroup("app", groups));
        assertTrue(FlowCalculator.owningGroup("application-0", groups) == null);
        assertTrue(FlowCalculator.owningGroup(null, groups) == null);
    }

    private static ClusterSample chainSample(long t, long inEnd, long relayCommitted, long outEnd, long sinkCommitted) {
        Tp in = new Tp("in", 0), out = new Tp("out", 0);
        return new ClusterSample(t, "c1", 1,
                Map.of("in", new TopicSample("in", 1, 1, Map.of(0, inEnd)),
                        "out", new TopicSample("out", 1, 1, Map.of(0, outEnd))),
                List.of(new GroupSample("relay", "STABLE", List.of(), Map.of(in, relayCommitted)),
                        new GroupSample("sink", "STABLE", List.of(), Map.of(out, sinkCommitted))),
                List.of(new ProducerSample(1, 0, in, (int) inEnd - 1, t, null),
                        new ProducerSample(2, 0, out, (int) outEnd - 1, t, "relay-0")),
                List.of());
    }
}
