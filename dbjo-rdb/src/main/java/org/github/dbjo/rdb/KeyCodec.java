package org.github.dbjo.rdb;

public interface KeyCodec<K> {
    byte[] encodeKey(K key);
    K decodeKey(byte[] keyBytes);

    static KeyCodec<String> stringUtf8() {
        return KeyCodecs.stringUtf8();
    }
}
