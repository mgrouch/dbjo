package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.rocksdb.ColumnFamilyHandle;

import java.util.List;

public final class UserSchema {
    public static final String USERS_CF  = "users";
    public static final String IDX_EMAIL = "users_email_idx";

    public static EntityDef<User, String> def(ColumnFamilyHandle usersCf, ColumnFamilyHandle emailIdxCf) {

        // proto storage codec + mapper => POJO codec
        Codec<org.github.dbjo.rdb.demo.generated.proto.User> protoCodec =
                ProtobufCodec.ofDefault(org.github.dbjo.rdb.demo.generated.proto.User.getDefaultInstance());

        Codec<User> userCodec =
                new MappedCodec<>(protoCodec, new UserProtoMapper());

        return new EntityDef<>(
                USERS_CF,
                usersCf,
                KeyCodec.stringUtf8(),
                userCodec,
                List.of(
                        IndexDef.unique(
                                IDX_EMAIL,
                                emailIdxCf,
                                User::getEmail,                 // returns String (V), nullable
                                IndexValueCodecs.STRING_UTF8    // encodes V -> byte[]
                        )
                )
        );
    }

    private UserSchema() {}
}
