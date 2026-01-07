package org.github.dbjo.rdb;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import java.util.Objects;
import java.util.function.Function;

public final class ProtobufPojoCodec<P, M extends MessageLite> implements Codec<P> {

    public interface ProtoMapper<P, M extends MessageLite> {
        M toProto(P pojo);
        P fromProto(M msg);
    }

    private final Parser<M> parser;
    private final Function<P, M> toProto;
    private final Function<M, P> fromProto;

    private ProtobufPojoCodec(Parser<M> parser, Function<P, M> toProto, Function<M, P> fromProto) {
        this.parser = Objects.requireNonNull(parser);
        this.toProto = Objects.requireNonNull(toProto);
        this.fromProto = Objects.requireNonNull(fromProto);
    }

    public static <P, M extends MessageLite> ProtobufPojoCodec<P, M> of(
            M defaultInstance,
            ProtoMapper<P, M> mapper
    ) {
        Objects.requireNonNull(defaultInstance, "defaultInstance");
        Objects.requireNonNull(mapper, "mapper");

        @SuppressWarnings("unchecked")
        Parser<M> p = (Parser<M>) defaultInstance.getParserForType(); // <-- key fix

        return new ProtobufPojoCodec<>(p, mapper::toProto, mapper::fromProto);
    }

    public static <P, M extends MessageLite> ProtobufPojoCodec<P, M> of(
            M defaultInstance,
            Function<P, M> toProto,
            Function<M, P> fromProto
    ) {
        Objects.requireNonNull(defaultInstance, "defaultInstance");

        @SuppressWarnings("unchecked")
        Parser<M> p = (Parser<M>) defaultInstance.getParserForType(); // <-- key fix

        return new ProtobufPojoCodec<>(p, toProto, fromProto);
    }

    @Override
    public byte[] encode(P value) {
        M msg = toProto.apply(value);
        if (msg == null) throw new IllegalArgumentException("toProto returned null");
        return msg.toByteArray();
    }

    @Override
    public P decode(byte[] bytes) {
        try {
            M msg = parser.parseFrom(bytes);
            return fromProto.apply(msg);
        } catch (Exception e) {
            throw new IllegalArgumentException("protobuf decode failed", e);
        }
    }
}
