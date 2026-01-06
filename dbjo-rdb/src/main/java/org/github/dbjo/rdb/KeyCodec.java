package org.github.dbjo.rdb;

import java.nio.charset.StandardCharsets;

public interface KeyCodec<K> {
    byte[] encodeKey(K key);
    K decodeKey(byte[] keyBytes);

    static KeyCodec<String> stringUtf8() {
        return new KeyCodec<>() {
            @Override public byte[] encodeKey(String key) { return key.getBytes(StandardCharsets.UTF_8); }
            @Override public String decodeKey(byte[] keyBytes) { return new String(keyBytes, StandardCharsets.UTF_8); }
        };
    }
}
