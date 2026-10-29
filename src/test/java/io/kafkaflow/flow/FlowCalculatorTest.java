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
                        List.of(new MemberSample("m1", "billing-1", "/10.0.0.1", Set.of("orders"), 2)),
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
}
