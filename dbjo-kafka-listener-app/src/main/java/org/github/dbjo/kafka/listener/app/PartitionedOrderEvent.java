package org.github.dbjo.kafka.listener.app;

import org.github.dbjo.kafka.avro.OrderEvent;
import org.github.dbjo.kafka.listener.PartitionedKafkaEvent;

public class PartitionedOrderEvent extends PartitionedKafkaEvent<OrderEvent> {
    public PartitionedOrderEvent(int partition, long offset, String key, long timestamp, OrderEvent event) {
        super(partition, offset, key, timestamp, event);
    }
}
