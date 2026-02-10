package org.github.dbjo.kafka.publisher;

public record KafkaPublishReceipt(
    String outboxId,
    String topic,
    int partition,
    long offset,
    long timestamp
) {
}
