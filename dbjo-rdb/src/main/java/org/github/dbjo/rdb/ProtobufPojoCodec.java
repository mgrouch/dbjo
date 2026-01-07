package org.github.dbjo.rdb;

import com.google.protobuf.MessageLite;

import java.util.Objects;

public final class ProtobufPojoCodec<P, M extends MessageLite> implements Codec<P> {

    public interface ProtoMapper<P, M extends MessageLite> {
        M toProto(P pojo);
        P fromProto(M proto);
    }

    private final Codec<M> protoCodec;          // <- was ProtobufCodec<M>
    private final ProtoMapper<P, M> mapper;

    public ProtobufPojoCodec(Codec<M> protoCodec, ProtoMapper<P, M> mapper) {
        this.protoCodec = Objects.requireNonNull(protoCodec);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public byte[] encode(P value) {
        return protoCodec.encode(mapper.toProto(value));
    }

    @Override
    public P decode(byte[] bytes) {
        return mapper.fromProto(protoCodec.decode(bytes));
    }
}
