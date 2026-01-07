package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.rocksdb.ColumnFamilyHandle;

import java.util.List;
import java.util.Map;

public final class UserDao extends IndexedRocksDao<User, String> {

    public static final String CF_USERS = "users";
    public static final String IDX_EMAIL = "users_email_idx";

    private static final List<IndexDef<User>> INDEXES = List.of(
            IndexDef.unique(IDX_EMAIL, IndexKeyCodec.utf8(), User::getEmail)
    );

    public UserDao(RocksSessions sessions,
                   ColumnFamilyHandle usersCf,
                   Map<String, ColumnFamilyHandle> indexCfs,
                   Codec<User> codec) {
        super(sessions, usersCf, KeyCodec.stringUtf8(), codec, indexCfs, INDEXES);
    }
}
