package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;

public final class UserDao extends IndexedRocksDao<User, String> {
    public UserDao(RocksSessions sessions, DaoRegistry registry, UserProtoMapper mapper) {
        super(sessions, registry.entity(UserSchema.def(registry.cf(UserSchema.USERS_CF), mapper)));
    }
}
