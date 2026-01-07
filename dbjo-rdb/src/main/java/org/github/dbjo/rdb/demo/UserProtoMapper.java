package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.ProtoMapper;

public final class UserProtoMapper implements ProtoMapper<User, org.github.dbjo.rdb.demo.generated.proto.User> {

    @Override
    public org.github.dbjo.rdb.demo.generated.proto.User toProto(User u) {
        var b = org.github.dbjo.rdb.demo.generated.proto.User.newBuilder();

        // id: treat null as ""
        if (u.getId() != null) b.setId(u.getId());

        // optional fields: only set if non-null
        if (u.getEmail() != null) b.setEmail(u.getEmail());
        if (u.getName() != null)  b.setName(u.getName());

        return b.build();
    }

    @Override
    public User fromProto(org.github.dbjo.rdb.demo.generated.proto.User p) {
        var u = new User();
        u.setId(p.getId());

        u.setEmail(p.hasEmail() ? p.getEmail() : null);
        u.setName (p.hasName()  ? p.getName()  : null);

        return u;
    }
}
