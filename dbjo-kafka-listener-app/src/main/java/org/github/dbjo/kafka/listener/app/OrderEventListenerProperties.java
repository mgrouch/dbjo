package org.github.dbjo.kafka.listener.app;

import org.github.dbjo.kafka.listener.KafkaEventListenerProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dbjo.kafka.listener")
public class OrderEventListenerProperties extends KafkaEventListenerProperties {
    public OrderEventListenerProperties() {
        setBootstrapServers("localhost:9092");
        setTopic("order-events");
        setGroupId("order-event-listener-app");
    }
}
