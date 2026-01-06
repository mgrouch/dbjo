package org.github.dbjo.rdb;

public record IndexRange(String indexName, byte[] from, boolean fromInclusive,
                         byte[] to, boolean toInclusive) implements IndexPredicate {}