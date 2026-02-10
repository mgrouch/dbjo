package org.github.dbjo.kafka.listener;

import java.util.Properties;
import org.github.dbjo.kafka.avro.OrderEvent;

public class KafkaOrderEventListener extends KafkaEventListener<OrderEvent> {

    public KafkaOrderEventListener(String bootstrapServers, String topic, String groupId, int partition, int partitionCount) {
        super(bootstrapServers, topic, groupId, partition, partitionCount, OrderEvent.getClassSchema());
    }

    public KafkaOrderEventListener(Properties properties, String topic, int partition, int partitionCount) {
        super(properties, topic, partition, partitionCount, OrderEvent.getClassSchema());
    }
}
