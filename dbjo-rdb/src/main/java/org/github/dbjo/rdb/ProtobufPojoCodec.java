package org.github.dbjo.rdb;

import com.google.protobuf.MessageLite;

import java.util.Objects;
import java.util.function.Function;

public final class ProtobufPojoCodec<P, M extends MessageLite> implements Codec<P> {

    public interface ProtoMapper<P, M extends MessageLite> {
        M toProto(P pojo);
        P fromProto(M msg);
    }

    private final M defaultInstance;
    private final Function<P, M> toProto;
    private final Function<M, P> fromProto;

    private ProtobufPojoCodec(M defaultInstance, Function<P, M> toProto, Function<M, P> fromProto) {
        this.defaultInstance = Objects.requireNonNull(defaultInstance, "defaultInstance");
        this.toProto = Objects.requireNonNull(toProto, "toProto");
        this.fromProto = Objects.requireNonNull(fromProto, "fromProto");
    }

    public static <P, M extends MessageLite> ProtobufPojoCodec<P, M> of(
            M defaultInstance,
            ProtoMapper<P, M> mapper
    ) {
        Objects.requireNonNull(mapper, "mapper");
        return new ProtobufPojoCodec<>(defaultInstance, mapper::toProto, mapper::fromProto);
    }

    public static <P, M extends MessageLite> ProtobufPojoCodec<P, M> of(
            M defaultInstance,
            Function<P, M> toProto,
            Function<M, P> fromProto
    ) {
        return new ProtobufPojoCodec<>(defaultInstance, toProto, fromProto);
    }

    @Override
    public byte[] encode(P value) {
        if (value == null) return null; // or throw, depending on your Codec contract
        M msg = toProto.apply(value);
        if (msg == null) throw new IllegalArgumentException("toProto returned null");
        return msg.toByteArray();
    }

    @Override
    public P decode(byte[] bytes) {
        if (bytes == null) return null; // or throw, depending on your Codec contract
        try {
            @SuppressWarnings("unchecked")
            M msg = (M) defaultInstance.getParserForType().parseFrom(bytes);
            return fromProto.apply(msg);
        } catch (Exception e) {
            throw new IllegalArgumentException("protobuf decode failed", e);
        }
    }
}
