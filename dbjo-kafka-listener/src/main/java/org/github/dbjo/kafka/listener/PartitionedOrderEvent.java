package org.github.dbjo.kafka.listener;

import org.github.dbjo.kafka.avro.OrderEvent;

public record PartitionedOrderEvent(int partition, long offset, String key, long timestamp, OrderEvent event) {
    public PartitionedOrderEvent {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
    }
}
