package org.github.dbjo.rdb;

import com.google.protobuf.MessageLite;

public interface ProtoMapper<P, M extends MessageLite> {
    M toProto(P pojo);
    P fromProto(M msg);
}
