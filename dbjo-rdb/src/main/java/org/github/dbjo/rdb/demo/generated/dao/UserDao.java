package org.github.dbjo.rdb.demo.generated.dao;

import org.github.dbjo.rdb.*;
import org.github.dbjo.rdb.demo.UserSchema;
import org.github.dbjo.rdb.demo.generated.entity.User;

public final class UserDao extends IndexedRocksDao<User, String> {
    public UserDao(RocksSessions sessions, DaoRegistry registry) {
        super(sessions, registry.entity(
                UserSchema.def(registry.cf(UserSchema.USERS_CF))
        ));
    }
}
