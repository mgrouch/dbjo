package org.github.dbjo.kafka.publisher.app;

import java.util.Objects;
import java.util.Properties;
import org.github.dbjo.kafka.avro.OrderEvent;
import org.github.dbjo.kafka.MutablePartitionKey;
import org.github.dbjo.kafka.publisher.KafkaEventPublisher;

public class KafkaOrderEventPublisher extends KafkaEventPublisher<OrderEvent> {
    public KafkaOrderEventPublisher(String bootstrapServers, String topic, int partitionCount) {
        super(bootstrapServers, topic, partitionCount, OrderEvent.getClassSchema());
    }

    public KafkaOrderEventPublisher(Properties properties, String topic, int partitionCount) {
        super(properties, topic, partitionCount, OrderEvent.getClassSchema());
    }

    public void publish(OrderEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        String productId = Objects.toString(event.getProductId(), null);
        publish(event, new MutablePartitionKey(productId));
    }

}
