package org.github.dbjo.kafka.publisher.app;

import java.util.Objects;
import java.util.Properties;
import java.util.List;
import org.github.dbjo.kafka.avro.OrderEvent;
import org.github.dbjo.kafka.MutablePartitionKey;
import org.github.dbjo.kafka.publisher.KafkaEventPublisher;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;

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
        String productId = Objects.toString(event.getProductId(), "");
        publish(event, new MutablePartitionKey(productId));
    }

    public List<KafkaPublishReceipt> publishEventsBatchInTransaction(List<OrderEvent> events) {
        return publishBatchInTransaction(events, this::partitionByProductId);
    }

    private MutablePartitionKey partitionByProductId(OrderEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        String productId = Objects.toString(event.getProductId(), "");
        return new MutablePartitionKey(productId);
    }

}
