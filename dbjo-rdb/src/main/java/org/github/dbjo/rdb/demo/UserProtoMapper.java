package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.ProtobufPojoCodec;

public final class UserProtoMapper
        implements ProtobufPojoCodec.ProtoMapper<User, org.github.dbjo.rdb.demo.generated.proto.User> {

    @Override
    public org.github.dbjo.rdb.demo.generated.proto.User toProto(User pojo) {
        var b = org.github.dbjo.rdb.demo.generated.proto.User.newBuilder();

        if (pojo.getId() != null) b.setId(pojo.getId());
        if (pojo.getEmail() != null) b.setEmail(pojo.getEmail());
        if (pojo.getName() != null) b.setName(pojo.getName());

        return b.build();
    }

    @Override
    public User fromProto(org.github.dbjo.rdb.demo.generated.proto.User p) {
        User u = new User();
        u.setId(p.getId());
        u.setEmail(p.hasEmail() ? p.getEmail() : null);
        u.setName(p.hasName() ? p.getName() : null);
        return u;
    }
}
