package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.rocksdb.ColumnFamilyHandle;

import java.util.List;

public final class UserSchema {
    public static final String USERS_CF  = "users";
    public static final String IDX_EMAIL = "users_email_idx";

    public static EntityDef<User, String> def(
            ColumnFamilyHandle usersCf,
            ProtobufPojoCodec.ProtoMapper<User, org.github.dbjo.rdb.demo.generated.proto.User> userProtoMapper
    ) {
        Codec<org.github.dbjo.rdb.demo.generated.proto.User> protoCodec =
                ProtobufCodec.ofDefault(org.github.dbjo.rdb.demo.generated.proto.User.getDefaultInstance());

        Codec<User> userCodec = new ProtobufPojoCodec<>(protoCodec, userProtoMapper);

        return new EntityDef<>(
                USERS_CF,
                usersCf,
                KeyCodec.stringUtf8(),
                userCodec,
                List.of(
                        // IMPORTANT: extractor returns String, codec encodes it -> fixes your “Required V provided byte[]”
                        IndexDef.unique(IDX_EMAIL, IndexKeyCodec.stringUtf8(), User::getEmail)
                )
        );
    }

    private UserSchema() {}
}
