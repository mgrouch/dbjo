package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.rocksdb.ColumnFamilyHandle;

import java.nio.charset.StandardCharsets;
import java.util.List;

public final class UserSchema {
    public static final String USERS_CF = "users";
    public static final String IDX_EMAIL = "users_email_idx";

    public static EntityDef<User, String> def(ColumnFamilyHandle usersCf, ColumnFamilyHandle emailIdxCf) {
        // protobuf codec (your DAOs store protobuf bytes directly)
        Codec<User> userCodec = ProtobufCodec.ofDefault(User.getDefaultInstance());

        return new EntityDef<>(
                USERS_CF,
                usersCf,
                KeyCodec.stringUtf8(),
                userCodec,
                List.of(
                        IndexDef.unique(
                                IDX_EMAIL,
                                emailIdxCf,
                                u -> {
                                    // Works for BOTH: optional string (presence) and non-optional string.
                                    // In proto3, unset string reads back as "".
                                    String email = u.getEmail();
                                    return email == null || email.isEmpty()
                                            ? null
                                            : email.getBytes(StandardCharsets.UTF_8);
                                }
                        )
                )
        );
    }

    private UserSchema() {}
}
