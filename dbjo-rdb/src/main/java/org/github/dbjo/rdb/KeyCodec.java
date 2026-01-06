package org.github.dbjo.rdb;

public interface KeyCodec<K> {
    byte[] encodeKey(K key);
    K decodeKey(byte[] bytes);
}
