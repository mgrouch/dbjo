package org.github.dbjo.meta.features;

public interface Partitioned {
    String getPartitionKey();

    void setPartitionKey(String partitionKey);
}
