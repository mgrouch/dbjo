package org.github.dbjo.kafka.publisher;

import org.github.dbjo.kafka.avro.OrderEvent;

public class KafkaOrderEventPublisher {
    public void publish(OrderEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
    }
}
