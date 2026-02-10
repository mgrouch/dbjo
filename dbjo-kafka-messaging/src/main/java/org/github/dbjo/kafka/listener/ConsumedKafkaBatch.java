package org.github.dbjo.kafka.listener;

import java.util.List;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public record ConsumedKafkaBatch<T extends SpecificRecord>(
    List<PartitionedKafkaEvent<T>> events,
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
    ConsumerGroupMetadata consumerGroupMetadata
) {
}
