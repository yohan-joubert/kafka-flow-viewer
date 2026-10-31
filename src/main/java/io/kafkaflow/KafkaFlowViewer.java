package io.kafkaflow;

import io.kafkaflow.config.AppConfig;
import io.kafkaflow.flow.FlowCalculator;
import io.kafkaflow.flow.FlowSnapshot;
import io.kafkaflow.source.ClusterSample;
import io.kafkaflow.source.ClusterSource;
import io.kafkaflow.source.KafkaClusterSource;
import io.kafkaflow.source.SimulatedClusterSource;
import io.kafkaflow.web.Json;
import io.kafkaflow.web.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Entry point: samples the cluster on a schedule and streams flow snapshots to the web UI. */
public final class KafkaFlowViewer {

    private static final Logger log = LoggerFactory.getLogger(KafkaFlowViewer.class);

    private final AppConfig config;
    private final ClusterSource source;
    private final FlowCalculator calculator;
    private final AtomicReference<FlowSnapshot> latest = new AtomicReference<>();
    private final AtomicReference<String> latestJson = new AtomicReference<>();
    private final AtomicReference<Map<String, List<FlowSnapshot.PartitionRate>>> partitions =
            new AtomicReference<>(Map.of());
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "cluster-poller");
        t.setDaemon(true);
        return t;
    });
    private WebServer web;
    private int consecutiveFailures;

    public KafkaFlowViewer(AppConfig config, ClusterSource source) {
        this.config = config;
        this.source = source;
        this.calculator = new FlowCalculator(config.rateSmoothing());
    }

    public static void main(String[] args) throws Exception {
        AppConfig config;
        try {
            config = AppConfig.fromArgs(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println(AppConfig.usage());
            System.exit(2);
            return;
        }
        if (config.help()) {
            System.out.println(AppConfig.usage());
            return;
        }

        ClusterSource source = config.demo() ? new SimulatedClusterSource() : new KafkaClusterSource(config);
        KafkaFlowViewer app = new KafkaFlowViewer(config, source);
        Runtime.getRuntime().addShutdownHook(new Thread(app::stop, "shutdown"));
        app.start();

        String host = config.httpHost().equals("0.0.0.0") ? "localhost" : config.httpHost();
        log.info("Watching {} - open http://{}:{}/", source.describe(), host, app.web.port());
        Thread.currentThread().join();
    }

    public void start() throws Exception {
        web = new WebServer(config.httpHost(), config.httpPort(), latestJson::get, this::partitionsJson);
        web.start();
        long interval = config.pollInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::pollOnce, 0, interval, TimeUnit.MILLISECONDS);
    }

    void pollOnce() {
        long interval = config.pollInterval().toMillis();
        FlowSnapshot snapshot;
        try {
            long t0 = System.nanoTime();
            ClusterSample sample = source.sample();
            snapshot = calculator.update(sample, source.describe(), interval);
            partitions.set(calculator.partitions());
            long ms = (System.nanoTime() - t0) / 1_000_000;
            if (consecutiveFailures > 0) log.info("Connection to {} restored", source.describe());
            consecutiveFailures = 0;
            log.debug("Sampled {} topics, {} groups, {} producers in {} ms", snapshot.topics().size(),
                    snapshot.groups().size(), snapshot.producers().size(), ms);
            if (ms > interval) {
                log.warn("Sampling took {} ms, longer than poll.interval.ms={}; consider a larger interval or topic filters",
                        ms, interval);
            }
        } catch (Exception e) {
            consecutiveFailures++;
            String msg = describeError(e);
            if (consecutiveFailures == 1 || consecutiveFailures % 30 == 0) {
                log.warn("Cannot sample {}: {}", source.describe(), msg);
            }
            snapshot = FlowSnapshot.error(source.describe(), msg, latest.get(), interval);
        } catch (Throwable t) {
            log.error("Unexpected error while polling", t);
            return;
        }
        latest.set(snapshot);
        String json = Json.write(snapshot);
        latestJson.set(json);
        web.publish(json);
    }

    private String partitionsJson(String node) {
        FlowSnapshot s = latest.get();
        return Json.write(Map.of(
                "node", node,
                "timestamp", s == null ? 0 : s.timestamp(),
                "partitions", partitions.get().getOrDefault(node, List.of())));
    }

    private static String describeError(Throwable e) {
        String m = e.getMessage();
        if (e instanceof java.util.concurrent.TimeoutException || (m != null && m.contains("Timed out"))) {
            return "Timed out talking to the cluster - check bootstrap servers, network and security settings";
        }
        return e.getClass().getSimpleName() + (m == null ? "" : ": " + m);
    }

    public void stop() {
        scheduler.shutdownNow();
        if (web != null) web.close();
        source.close();
    }
}
