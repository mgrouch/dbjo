package org.github.dbjo.rdb;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import java.util.Objects;

public final class ProtobufCodec<T extends MessageLite> implements Codec<T> {
    private final Parser<T> parser;

    public ProtobufCodec(Parser<T> parser) {
        this.parser = Objects.requireNonNull(parser, "parser");
    }

    /** Convenient factory if you have a default instance. */
    @SuppressWarnings("unchecked")
    public static <T extends MessageLite> ProtobufCodec<T> ofDefault(T defaultInstance) {
        return new ProtobufCodec<>((Parser<T>) defaultInstance.getParserForType());
    }

    @Override
    public byte[] encode(T value) {
        return value.toByteArray();
    }

    @Override
    public T decode(byte[] bytes) {
        try {
            return parser.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("protobuf decode failed", e);
        }
    }
}
