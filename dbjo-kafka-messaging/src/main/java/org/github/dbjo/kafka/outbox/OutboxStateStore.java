package org.github.dbjo.kafka.outbox;

import java.util.Collection;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;

public interface OutboxStateStore {
    void markPublished(Collection<KafkaPublishReceipt> receipts);
}
