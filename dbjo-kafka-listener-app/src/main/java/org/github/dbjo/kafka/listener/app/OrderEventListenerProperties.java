package org.github.dbjo.kafka.listener.app;

import org.github.dbjo.kafka.listener.KafkaEventListenerProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dbjo.kafka.listener")
public class OrderEventListenerProperties extends KafkaEventListenerProperties {
    private int partition;

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }
}
