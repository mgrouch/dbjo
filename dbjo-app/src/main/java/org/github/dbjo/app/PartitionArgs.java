package org.github.dbjo.app;

import org.springframework.boot.ApplicationArguments;

import java.util.List;

public record PartitionArgs(int partitionNum, int totalPartitions) {
    public static final String PARTITION_NUM_ARG = "partition-num";
    public static final String TOTAL_PARTITIONS_ARG = "total-partitions";

    public static PartitionArgs from(ApplicationArguments args) {
        int partitionNum = parseIntArg(args, PARTITION_NUM_ARG, 0);
        int totalPartitions = parseIntArg(args, TOTAL_PARTITIONS_ARG, 1);
        if (totalPartitions <= 0) {
            throw new IllegalArgumentException("total-partitions must be >= 1");
        }
        if (partitionNum < 0 || partitionNum >= totalPartitions) {
            throw new IllegalArgumentException("partition-num must be between 0 and total-partitions-1");
        }
        return new PartitionArgs(partitionNum, totalPartitions);
    }

    private static int parseIntArg(ApplicationArguments args, String name, int defaultValue) {
        if (args == null) {
            return defaultValue;
        }
        List<String> values = args.getOptionValues(name);
        if (values == null || values.isEmpty()) {
            return defaultValue;
        }
        String value = values.get(0);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid value for --" + name + ": " + value, ex);
        }
    }
}
