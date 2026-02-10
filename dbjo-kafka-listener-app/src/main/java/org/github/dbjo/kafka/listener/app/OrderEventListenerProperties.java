package org.github.dbjo.kafka.listener.app;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.github.dbjo.kafka.listener.KafkaEventListenerProperties;

@ConfigurationProperties(prefix = "dbjo.kafka.listener")
public class OrderEventListenerProperties extends KafkaEventListenerProperties {
    private String outputTopic = "processed-order-events";
    private String producerTransactionalIdPrefix = "order-event-listener-tx";

    public OrderEventListenerProperties() {
        setBootstrapServers("localhost:9092");
        setTopic("order-events");
        setGroupId("order-event-listener-app");
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public String getProducerTransactionalIdPrefix() {
        return producerTransactionalIdPrefix;
    }

    public void setProducerTransactionalIdPrefix(String producerTransactionalIdPrefix) {
        this.producerTransactionalIdPrefix = producerTransactionalIdPrefix;
    }
}
