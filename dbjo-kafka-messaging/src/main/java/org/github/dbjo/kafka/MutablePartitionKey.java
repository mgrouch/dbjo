package org.github.dbjo.kafka;

import org.github.dbjo.meta.features.Partitioned;

public class MutablePartitionKey implements Partitioned {
    private String partitionKey;

    public MutablePartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    @Override
    public String getPartitionKey() {
        return partitionKey;
    }

    @Override
    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }
}
