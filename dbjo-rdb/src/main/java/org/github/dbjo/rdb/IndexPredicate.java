package org.github.dbjo.rdb;

public sealed interface IndexPredicate permits IndexPredicate.Eq {
    record Eq(String indexName, byte[] valueBytes) implements IndexPredicate {}
}