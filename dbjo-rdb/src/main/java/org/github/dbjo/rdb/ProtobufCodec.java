package org.github.dbjo.rdb;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import java.util.Arrays;

public final class ProtobufCodec<T extends MessageLite> implements Codec<T> {
    private static final byte[] MAGIC = new byte[] { 'D','B','J','O', 1 };

    private final Parser<T> parser;

    private ProtobufCodec(Parser<T> parser) {
        this.parser = parser;
    }

    public static <T extends MessageLite> ProtobufCodec<T> ofDefault(T defaultInstance) {
        @SuppressWarnings("unchecked")
        Parser<T> p = (Parser<T>) defaultInstance.getParserForType();
        return new ProtobufCodec<>(p);
    }

    @Override
    public byte[] encode(T value) {
        byte[] body = value.toByteArray();
        byte[] out = new byte[MAGIC.length + body.length];
        System.arraycopy(MAGIC, 0, out, 0, MAGIC.length);
        System.arraycopy(body, 0, out, MAGIC.length, body.length);
        return out;
    }

    @Override
    public T decode(byte[] bytes) {
        try {
            if (bytes.length < MAGIC.length) {
                throw new IllegalArgumentException("protobuf decode failed: too short (" + bytes.length + " bytes)");
            }
            if (!startsWith(bytes, MAGIC)) {
                throw new IllegalArgumentException("protobuf decode failed: missing MAGIC header (likely old/non-protobuf data)");
            }
            byte[] body = Arrays.copyOfRange(bytes, MAGIC.length, bytes.length);
            return parser.parseFrom(body);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("protobuf decode failed", e);
        }
    }

    private static boolean startsWith(byte[] a, byte[] prefix) {
        for (int i = 0; i < prefix.length; i++) {
            if (a[i] != prefix[i]) return false;
        }
        return true;
    }
}
