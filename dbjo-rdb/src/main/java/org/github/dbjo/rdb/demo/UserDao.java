package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;

public final class UserDao extends RocksDao<User, String> {
    public UserDao(RocksSessions sessions, EntityDef<User, String> def) { super(sessions, def); }

    public java.util.stream.Stream<User> findByEmail(String email) {
        if (email == null || email.isBlank()) return java.util.stream.Stream.empty();
        byte[] b = email.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        var q = Query.<String>builder().where(new IndexPredicate.Eq(UserSchema.IDX_EMAIL, b)).build();
        return stream(q).map(java.util.Map.Entry::getValue);
    }
}
