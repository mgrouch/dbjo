package org.github.dbjo.kafka;

import org.github.dbjo.meta.features.Partitioned;

import java.io.Serializable;

public class MutablePartitionKey implements Partitioned, Serializable {
    private String partitionKey;

    public MutablePartitionKey() {
    }

    public MutablePartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    @Override
    public String getPartitionKey() {
        return partitionKey;
    }

    @Override
    public void setPartitionKey(String partitionKey) {
        if (this.partitionKey == null) {
            this.partitionKey = partitionKey;
        }
    }
}
