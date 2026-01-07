package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.rocksdb.ColumnFamilyHandle;

import java.util.List;

public final class UserSchema {
    public static final String USERS_CF  = "users";
    public static final String IDX_EMAIL = "users_email_idx";

    public static EntityDef<User, String> def(ColumnFamilyHandle usersCf, ColumnFamilyHandle emailIdxCf) {
        Codec<User> userCodec = ProtobufCodec.ofDefault(User.getDefaultInstance());

        return new EntityDef<>(
                USERS_CF,
                usersCf,
                KeyCodec.stringUtf8(),
                userCodec,
                List.of(
                        IndexDef.unique(
                                IDX_EMAIL,
                                IndexKeyCodec.stringUtf8(),
                                User::getEmail
                        )
                )
        );
    }

    private UserSchema() {}
}
