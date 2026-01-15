package org.github.dbjo.rdb;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class KeyCodecs {
    private KeyCodecs() {}

    public static KeyCodec<String> stringUtf8() {
        return new KeyCodec<>() {
            @Override public byte[] encodeKey(String key) {
                if (key == null) throw new IllegalArgumentException("key is null");
                return key.getBytes(StandardCharsets.UTF_8);
            }
            @Override public String decodeKey(byte[] keyBytes) {
                if (keyBytes == null) throw new IllegalArgumentException("keyBytes is null");
                return new String(keyBytes, StandardCharsets.UTF_8);
            }
        };
    }

    /**
     * Sortable 8-byte signed long key.
     * Encoding flips the sign bit so lexicographic byte order matches numeric order.
     */
    public static KeyCodec<Long> int64() {
        return new KeyCodec<>() {
            @Override public byte[] encodeKey(Long key) {
                if (key == null) throw new IllegalArgumentException("key is null");
                long v = key ^ 0x8000_0000_0000_0000L;
                byte[] b = new byte[8];
                b[0] = (byte)(v >>> 56);
                b[1] = (byte)(v >>> 48);
                b[2] = (byte)(v >>> 40);
                b[3] = (byte)(v >>> 32);
                b[4] = (byte)(v >>> 24);
                b[5] = (byte)(v >>> 16);
                b[6] = (byte)(v >>>  8);
                b[7] = (byte)(v);
                return b;
            }
            @Override public Long decodeKey(byte[] keyBytes) {
                if (keyBytes == null) throw new IllegalArgumentException("keyBytes is null");
                if (keyBytes.length != 8) throw new IllegalArgumentException("Expected 8 bytes, got " + keyBytes.length);
                long v =
                        ((long)(keyBytes[0] & 0xFF) << 56) |
                                ((long)(keyBytes[1] & 0xFF) << 48) |
                                ((long)(keyBytes[2] & 0xFF) << 40) |
                                ((long)(keyBytes[3] & 0xFF) << 32) |
                                ((long)(keyBytes[4] & 0xFF) << 24) |
                                ((long)(keyBytes[5] & 0xFF) << 16) |
                                ((long)(keyBytes[6] & 0xFF) <<  8) |
                                ((long)(keyBytes[7] & 0xFF));
                return v ^ 0x8000_0000_0000_0000L;
            }
        };
    }

    /** Sortable 4-byte signed int key (big-endian + sign-bit flip). */
    public static KeyCodec<Integer> int32() {
        return new KeyCodec<>() {
            @Override public byte[] encodeKey(Integer key) {
                if (key == null) throw new IllegalArgumentException("key is null");
                int v = key ^ 0x8000_0000;
                byte[] b = new byte[4];
                b[0] = (byte)(v >>> 24);
                b[1] = (byte)(v >>> 16);
                b[2] = (byte)(v >>>  8);
                b[3] = (byte)(v);
                return b;
            }
            @Override public Integer decodeKey(byte[] keyBytes) {
                if (keyBytes == null) throw new IllegalArgumentException("keyBytes is null");
                if (keyBytes.length != 4) throw new IllegalArgumentException("Expected 4 bytes, got " + keyBytes.length);
                int v =
                        ((keyBytes[0] & 0xFF) << 24) |
                                ((keyBytes[1] & 0xFF) << 16) |
                                ((keyBytes[2] & 0xFF) <<  8) |
                                ((keyBytes[3] & 0xFF));
                return v ^ 0x8000_0000;
            }
        };
    }

    public static KeyCodec<byte[]> bytes() {
        return new KeyCodec<>() {
            @Override public byte[] encodeKey(byte[] key) {
                if (key == null) throw new IllegalArgumentException("key is null");
                return key;
            }
            @Override public byte[] decodeKey(byte[] keyBytes) {
                if (keyBytes == null) throw new IllegalArgumentException("keyBytes is null");
                return keyBytes;
            }
        };
    }

    /** UUID as 16 bytes: two sortable longs. */
    public static KeyCodec<UUID> uuid() {
        return new KeyCodec<>() {
            @Override public byte[] encodeKey(UUID key) {
                if (key == null) throw new IllegalArgumentException("key is null");
                byte[] b = new byte[16];
                writeSortableLong(b, 0, key.getMostSignificantBits());
                writeSortableLong(b, 8, key.getLeastSignificantBits());
                return b;
            }
            @Override public UUID decodeKey(byte[] keyBytes) {
                if (keyBytes == null) throw new IllegalArgumentException("keyBytes is null");
                if (keyBytes.length != 16) throw new IllegalArgumentException("Expected 16 bytes, got " + keyBytes.length);
                long msb = readSortableLong(keyBytes, 0);
                long lsb = readSortableLong(keyBytes, 8);
                return new UUID(msb, lsb);
            }
        };
    }

    private static void writeSortableLong(byte[] b, int off, long x) {
        long v = x ^ 0x8000_0000_0000_0000L;
        b[off]     = (byte)(v >>> 56);
        b[off + 1] = (byte)(v >>> 48);
        b[off + 2] = (byte)(v >>> 40);
        b[off + 3] = (byte)(v >>> 32);
        b[off + 4] = (byte)(v >>> 24);
        b[off + 5] = (byte)(v >>> 16);
        b[off + 6] = (byte)(v >>>  8);
        b[off + 7] = (byte)(v);
    }

    private static long readSortableLong(byte[] b, int off) {
        long v =
                ((long)(b[off] & 0xFF) << 56) |
                        ((long)(b[off + 1] & 0xFF) << 48) |
                        ((long)(b[off + 2] & 0xFF) << 40) |
                        ((long)(b[off + 3] & 0xFF) << 32) |
                        ((long)(b[off + 4] & 0xFF) << 24) |
                        ((long)(b[off + 5] & 0xFF) << 16) |
                        ((long)(b[off + 6] & 0xFF) <<  8) |
                        ((long)(b[off + 7] & 0xFF));
        return v ^ 0x8000_0000_0000_0000L;
    }
}
