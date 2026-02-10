package org.github.dbjo.kafka.publisher;

import java.util.Objects;
import java.util.Properties;
import org.github.dbjo.kafka.avro.OrderEvent;
import org.github.dbjo.meta.features.Partitioned;

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

    private static final class MutablePartitionKey implements Partitioned {
        private String partitionKey;

        private MutablePartitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
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
}
