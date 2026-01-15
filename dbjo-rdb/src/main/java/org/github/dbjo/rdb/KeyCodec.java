package org.github.dbjo.rdb;

import java.util.UUID;

public interface KeyCodec<K> {
    byte[] encodeKey(K key);
    K decodeKey(byte[] keyBytes);

    static KeyCodec<String> stringUtf8() {
        return KeyCodecs.stringUtf8();
    }

    // --- Add these ---

    /** 8-byte sortable signed long (big-endian with sign-bit flip). */
    static KeyCodec<Long> int64() {
        return KeyCodecs.int64();
    }

    /** 4-byte sortable signed int (big-endian with sign-bit flip). */
    static KeyCodec<Integer> int32() {
        return KeyCodecs.int32();
    }

    /** Identity codec for raw bytes. */
    static KeyCodec<byte[]> bytes() {
        return KeyCodecs.bytes();
    }

    /** 16-byte UUID codec (two 8-byte sortable longs). */
    static KeyCodec<UUID> uuid() {
        return KeyCodecs.uuid();
    }
}
