package org.github.dbjo.kafka.listener;

import org.github.dbjo.kafka.avro.OrderEvent;

public class KafkaOrderEventListener {
    public void onMessage(OrderEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
    }
}
