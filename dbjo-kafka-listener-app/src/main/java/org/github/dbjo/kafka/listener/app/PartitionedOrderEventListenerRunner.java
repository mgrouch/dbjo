package org.github.dbjo.kafka.listener.app;

import org.github.dbjo.kafka.listener.KafkaOrderEventListener;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class PartitionedOrderEventListenerRunner implements CommandLineRunner {
    private final OrderEventListenerProperties properties;

    public PartitionedOrderEventListenerRunner(OrderEventListenerProperties properties) {
        this.properties = properties;
    }

    @Override
    public void run(String... args) {
        try (KafkaOrderEventListener listener = new KafkaOrderEventListener(
                properties.getBootstrapServers(),
                properties.getTopic(),
                properties.getGroupId(),
                properties.getPartition(),
                properties.getPartitionCount())) {
            PartitionedKafkaListenerWorker worker = new SampleOrderEventWorker(listener, properties);
            worker.run(properties.getMaxPollIterations());
        }
    }
}
