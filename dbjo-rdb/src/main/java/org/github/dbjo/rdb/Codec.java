package org.github.dbjo.rdb;

public interface Codec<T> {
    byte[] encode(T value);
    T decode(byte[] bytes);
}
