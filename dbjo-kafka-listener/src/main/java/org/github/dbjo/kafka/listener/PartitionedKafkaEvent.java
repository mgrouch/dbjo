package org.github.dbjo.kafka.listener;

import java.util.Objects;
import org.github.dbjo.meta.features.Partitioned;

public class PartitionedKafkaEvent<T> implements Partitioned {
    private final int partition;
    private final long offset;
    private String partitionKey;
    private final long timestamp;
    private final T event;

    public PartitionedKafkaEvent(int partition, long offset, String partitionKey, long timestamp, T event) {
        this.partition = partition;
        this.offset = offset;
        this.partitionKey = partitionKey;
        this.timestamp = timestamp;
        this.event = Objects.requireNonNull(event, "event must not be null");
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public long timestamp() {
        return timestamp;
    }

    public String key() {
        return partitionKey;
    }

    public T event() {
        return event;
    }

    @Override
    public String getPartitionKey() {
        return partitionKey;
    }

    @Override
    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }
}
