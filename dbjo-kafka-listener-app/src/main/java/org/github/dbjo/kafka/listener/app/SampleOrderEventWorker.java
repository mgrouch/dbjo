package org.github.dbjo.kafka.listener.app;

import java.util.List;
import org.github.dbjo.kafka.listener.KafkaOrderEventListener;
import org.github.dbjo.kafka.listener.PartitionedOrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleOrderEventWorker extends PartitionedKafkaListenerWorker {
    private static final Logger LOG = LoggerFactory.getLogger(SampleOrderEventWorker.class);

    public SampleOrderEventWorker(KafkaOrderEventListener listener, OrderEventListenerProperties properties) {
        super(listener, properties.getPollTimeout());
    }

    @Override
    protected void processBatch(List<PartitionedOrderEvent> events) {
        // Implement batch business logic here.
        LOG.info("Received batch size={} from partition={}", events.size(), events.getFirst().partition());
    }

    @Override
    protected void processMessage(PartitionedOrderEvent event) {
        // Implement per-message business logic here.
        LOG.info(
                "Processing message partition={} offset={} key={} productId={}",
                event.partition(),
                event.offset(),
                event.key(),
                event.event().getProductId());
    }
}
