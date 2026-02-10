package org.github.dbjo.kafka.listener;

import java.time.Duration;

public class KafkaEventListenerProperties {
    private String bootstrapServers;
    private String topic;
    private String groupId;
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
