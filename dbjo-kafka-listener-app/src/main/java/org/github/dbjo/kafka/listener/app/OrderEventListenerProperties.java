package org.github.dbjo.kafka.listener.app;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dbjo.kafka.listener")
public class OrderEventListenerProperties {
    private String bootstrapServers = "localhost:9092";
    private String topic = "order-events";
    private String groupId = "order-event-listener-app";
    private int partition;
    private int partitionCount = 1;
    private Duration pollTimeout = Duration.ofMillis(500);
    private long maxPollIterations = -1L;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public long getMaxPollIterations() {
        return maxPollIterations;
    }

    public void setMaxPollIterations(long maxPollIterations) {
        this.maxPollIterations = maxPollIterations;
    }
}
