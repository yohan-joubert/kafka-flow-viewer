package io.kafkaflow.source;

/** Something that can take a {@link ClusterSample}: a real cluster or the built-in simulator. */
public interface ClusterSource extends AutoCloseable {

    ClusterSample sample() throws Exception;

    /** Short human description, shown in the UI header. */
    String describe();

    @Override
    default void close() {}
}
