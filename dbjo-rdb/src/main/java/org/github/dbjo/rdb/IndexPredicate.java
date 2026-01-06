package org.github.dbjo.rdb;

public sealed interface IndexPredicate
        permits IndexPredicate.Eq, IndexPredicate.Range {

    record Eq(String indexName, byte[] valueBytes) implements IndexPredicate {}

    record Range(
            String indexName,
            byte[] from, boolean fromInclusive,
            byte[] to,   boolean toInclusive
    ) implements IndexPredicate {}
}