package org.github.dbjo.rdb;

public interface IndexValueCodec<V> {
    byte[] encode(V value);
}
