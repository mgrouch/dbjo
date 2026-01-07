package org.github.dbjo.rdb;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public final class ProtobufCodec<M extends MessageLite> implements Codec<M> {
    private final Parser<M> parser;

    private ProtobufCodec(Parser<M> parser) {
        this.parser = parser;
    }

    public static <M extends MessageLite> ProtobufCodec<M> ofDefault(M defaultInstance) {
        @SuppressWarnings("unchecked")
        Parser<M> p = (Parser<M>) defaultInstance.getParserForType();
        return new ProtobufCodec<>(p);
    }

    @Override
    public byte[] encode(M value) {
        return value.toByteArray();
    }

    @Override
    public M decode(byte[] bytes) {
        try {
            return parser.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("protobuf decode failed", e);
        }
    }
}
