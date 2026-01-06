package org.github.dbjo.rdb;

public record IndexEq(String indexName, byte[] valueKey) implements IndexPredicate {}