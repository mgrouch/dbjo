package org.github.dbjo.rdb.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.github.dbjo.rdb.*;
import org.rocksdb.ColumnFamilyHandle;

import java.nio.charset.StandardCharsets;
import java.util.List;

public final class UserSchema {
    public static final String USERS_CF = "users";
    public static final String IDX_EMAIL = "users_email_idx";

    public static EntityDef<User, String> def(ObjectMapper om, ColumnFamilyHandle usersCf, ColumnFamilyHandle emailIdxCf) {
        var userCodec = new JacksonCodec<>(om, User.class);

        return new EntityDef<>(
                "users",
                usersCf,
                KeyCodec.stringUtf8(),
                userCodec,
                List.of(
                        IndexDef.unique(
                                IDX_EMAIL,
                                emailIdxCf,
                                u -> u.email() == null ? null : u.email().getBytes(StandardCharsets.UTF_8)
                        )
                )
        );
    }
}
