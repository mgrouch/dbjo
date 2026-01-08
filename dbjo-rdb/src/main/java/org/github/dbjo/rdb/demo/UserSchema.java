package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.github.dbjo.rdb.demo.generated.entity.User;
import org.github.dbjo.rdb.demo.generated.protomap.UserProtoMapper;
import org.rocksdb.ColumnFamilyHandle;

import java.util.List;

public final class UserSchema {
    public static final String USERS_CF  = "users";
    public static final String IDX_EMAIL = "users_email_idx";

    public static EntityDef<User, String> def(ColumnFamilyHandle usersCf) {
        Codec<User> userCodec =
                ProtobufPojoCodec.of(
                        org.github.dbjo.rdb.demo.generated.proto.User.getDefaultInstance(), // <-- fix
                        new UserProtoMapper() // must implement ProtoMapper<User, generated.proto.User>
                );

        return new EntityDef<>(
                USERS_CF,
                usersCf,
                KeyCodec.stringUtf8(),
                userCodec,
                List.of(
                        IndexDef.unique(IDX_EMAIL, IndexKeyCodec.stringUtf8(), User::getEmail)
                )
        );
    }

    private UserSchema() {}
}
