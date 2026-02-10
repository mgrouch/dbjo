package org.github.dbjo.kafka.listener.app;

import java.util.List;

import org.github.dbjo.kafka.listener.PartitionedKafkaEvent;
import org.github.dbjo.kafka.listener.PartitionedKafkaListenerWorker;
import org.github.dbjo.kafka.avro.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleOrderEventWorker extends PartitionedKafkaListenerWorker<OrderEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(SampleOrderEventWorker.class);

    public SampleOrderEventWorker(KafkaOrderEventListener listener, OrderEventListenerProperties properties) {
        super(listener, properties.getPollTimeout());
    }

    @Override
    protected void processBatch(List<PartitionedKafkaEvent<OrderEvent>> events) {
        // Implement batch business logic here.
        LOG.info("Received batch size={} from partition={}", events.size(), events.getFirst().partition());
    }

    @Override
    protected void processMessage(PartitionedKafkaEvent<OrderEvent> event) {
        // Implement per-message business logic here.
        LOG.info(
                "Processing message partition={} offset={} key={} productId={}",
                event.partition(),
                event.offset(),
                event.key(),
                event.event().getProductId());
    }
}
