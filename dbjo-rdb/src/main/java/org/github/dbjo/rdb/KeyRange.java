package org.github.dbjo.rdb;

public record KeyRange<K>(K from, boolean fromInclusive, K to, boolean toInclusive) {}
