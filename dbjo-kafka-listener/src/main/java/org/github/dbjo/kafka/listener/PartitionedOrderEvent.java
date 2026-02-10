package org.github.dbjo.kafka.listener;

import org.github.dbjo.kafka.avro.OrderEvent;

public class PartitionedOrderEvent extends PartitionedKafkaEvent<OrderEvent> {
    public PartitionedOrderEvent(int partition, long offset, String key, long timestamp, OrderEvent event) {
        super(partition, offset, key, timestamp, event);
    }

    public String key() {
        return getPartitionKey();
    }
}
