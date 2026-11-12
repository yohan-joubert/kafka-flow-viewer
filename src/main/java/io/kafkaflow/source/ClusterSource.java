package io.kafkaflow.source;

/** Something that can take a {@link ClusterSample}: a real cluster or the built-in simulator. */
public interface ClusterSource extends AutoCloseable {

    ClusterSample sample() throws Exception;

    /**
     * Reads one message picked at random from the topic (random non-empty partition, random offset in it).
     * Returns null when the topic has no messages. May be called from any thread.
     */
    SampledMessage sampleMessage(String topic, int maxBytes) throws Exception;

    /** Short human description, shown in the UI header. */
    String describe();

    @Override
    default void close() {}
}
