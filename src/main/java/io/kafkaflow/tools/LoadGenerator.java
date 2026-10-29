package io.kafkaflow.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Generates some real traffic on a test cluster so there is something to look at:
 * a few producers and consumer groups on {@code demo.*} topics, one group deliberately slow.
 *
 * <pre>java -cp kafka-flow-viewer.jar io.kafkaflow.tools.LoadGenerator localhost:9092</pre>
 *
 * Do not point this at a production cluster: it creates topics and writes test messages.
 */
public final class LoadGenerator {

    public static void main(String[] args) throws Exception {
        String bootstrap = args.length > 0 ? args[0] : "localhost:9092";
        System.out.println("Generating demo traffic on " + bootstrap + " (Ctrl+C to stop)");

        createTopics(bootstrap,
                new NewTopic("demo.orders", 6, (short) 1),
                new NewTopic("demo.payments", 3, (short) 1),
                new NewTopic("demo.clicks", 12, (short) 1),
                new NewTopic("demo.notifications", 3, (short) 1));

        List<Thread> threads = new ArrayList<>();
        threads.add(producer(bootstrap, "checkout-1", "demo.orders", 120, null));
        threads.add(producer(bootstrap, "checkout-2", "demo.orders", 80, null));
        threads.add(producer(bootstrap, "payments", "demo.payments", 60, "payment-processor-0"));
        threads.add(producer(bootstrap, "web-tracker", "demo.clicks", 500, null));
        threads.add(producer(bootstrap, "notifier-feed", "demo.notifications", 30, null));

        threads.add(consumer(bootstrap, "billing", List.of("demo.orders", "demo.payments"), 0));
        threads.add(consumer(bootstrap, "analytics", List.of("demo.clicks"), 0));
        threads.add(consumer(bootstrap, "analytics", List.of("demo.clicks"), 0));
        threads.add(consumer(bootstrap, "mailer", List.of("demo.notifications"), 0));
        // Too slow on purpose: ~100 msg/s for ~200 msg/s of orders, so lag keeps growing.
        threads.add(consumer(bootstrap, "slow-reporting", List.of("demo.orders"), 100));

        for (Thread t : threads) t.join();
    }

    private static void createTopics(String bootstrap, NewTopic... topics) throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        try (Admin admin = Admin.create(p)) {
            for (NewTopic t : topics) {
                try {
                    admin.createTopics(List.of(t)).all().get();
                    System.out.println("Created topic " + t.name());
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) throw e;
                }
            }
        }
    }

    private static Thread producer(String bootstrap, String clientId, String topic, int ratePerSec, String txId) {
        return Thread.ofPlatform().name("producer-" + clientId).start(() -> {
            Properties p = new Properties();
            p.put("bootstrap.servers", bootstrap);
            p.put("client.id", clientId);
            p.put("enable.idempotence", "true"); // makes the producer visible to describeProducers
            p.put("linger.ms", "20");
            p.put("key.serializer", StringSerializer.class.getName());
            p.put("value.serializer", StringSerializer.class.getName());
            if (txId != null) p.put("transactional.id", txId);
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
                if (txId != null) producer.initTransactions();
                long n = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    long t0 = System.nanoTime();
                    int batch = Math.max(1, ratePerSec / 10);
                    if (txId != null) producer.beginTransaction();
                    for (int i = 0; i < batch; i++, n++) {
                        producer.send(new ProducerRecord<>(topic, "key-" + (n % 97), "{\"seq\":" + n + ",\"id\":\"" + UUID.randomUUID() + "\"}"));
                    }
                    if (txId != null) producer.commitTransaction();
                    sleepRemaining(t0, 100);
                }
            } catch (Exception e) {
                System.err.println(clientId + " stopped: " + e);
            }
        });
    }

    private static Thread consumer(String bootstrap, String groupId, List<String> topics, int maxPerSec) {
        return Thread.ofPlatform().name("consumer-" + groupId).start(() -> {
            Properties p = new Properties();
            p.put("bootstrap.servers", bootstrap);
            p.put("group.id", groupId);
            p.put("client.id", groupId + "-" + UUID.randomUUID().toString().substring(0, 4));
            p.put("auto.offset.reset", "latest");
            p.put("enable.auto.commit", "true");
            p.put("auto.commit.interval.ms", "1000");
            p.put("key.deserializer", StringDeserializer.class.getName());
            p.put("value.deserializer", StringDeserializer.class.getName());
            if (maxPerSec > 0) p.put("max.poll.records", String.valueOf(Math.max(1, maxPerSec / 10)));
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
                consumer.subscribe(topics);
                while (!Thread.currentThread().isInterrupted()) {
                    long t0 = System.nanoTime();
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                    if (maxPerSec > 0 && !records.isEmpty()) sleepRemaining(t0, 100);
                }
            } catch (Exception e) {
                System.err.println(groupId + " stopped: " + e);
            }
        });
    }

    private static void sleepRemaining(long startNanos, long periodMs) {
        long elapsed = (System.nanoTime() - startNanos) / 1_000_000;
        if (elapsed < periodMs) {
            try {
                Thread.sleep(periodMs - elapsed);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
