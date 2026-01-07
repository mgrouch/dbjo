package org.github.dbjo.rdb;

import com.google.protobuf.MessageLite;

public final class ProtobufPojoCodec<P, M extends MessageLite> implements Codec<P> {

    public interface ProtoMapper<P, M extends MessageLite> {
        M toProto(P pojo);
        P fromProto(M proto);
    }

    private final ProtobufCodec<M> protoCodec;
    private final ProtoMapper<P, M> mapper;

    public ProtobufPojoCodec(ProtobufCodec<M> protoCodec, ProtoMapper<P, M> mapper) {
        this.protoCodec = protoCodec;
        this.mapper = mapper;
    }

    @Override public byte[] encode(P value) {
        return protoCodec.encode(mapper.toProto(value));
    }

    @Override public P decode(byte[] bytes) {
        return mapper.fromProto(protoCodec.decode(bytes));
    }
}
