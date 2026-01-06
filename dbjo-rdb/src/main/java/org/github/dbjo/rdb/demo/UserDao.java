package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class UserDao extends AbstractRocksDao<User, String> {

    public static final String CF_USERS = "users";
    public static final String IDX_EMAIL = "users_email_idx";

    public UserDao(RocksSessions sessions,
                   ColumnFamilyHandle usersCf,
                   Map<String, ColumnFamilyHandle> indexCfs,
                   Codec<User> codec) {
        super(sessions, usersCf, KeyCodec.stringUtf8(), codec, indexCfs);
    }

    @Override
    protected void maintainIndexes(RocksWriteBatch batch, String key, User oldValueOrNull, User newValue) throws RocksDBException {
        if (oldValueOrNull != null && !oldValueOrNull.email().equals(newValue.email())) {
            batch.delete(indexCfs.get(IDX_EMAIL), emailIdxKey(oldValueOrNull.email(), key));
        }
        batch.put(indexCfs.get(IDX_EMAIL), emailIdxKey(newValue.email(), key), new byte[0]);
    }

    @Override
    protected void maintainIndexesOnDelete(RocksWriteBatch batch, String key, User oldValue) throws RocksDBException {
        batch.delete(indexCfs.get(IDX_EMAIL), emailIdxKey(oldValue.email(), key));
    }

    private byte[] emailIdxKey(String email, String id) {
        byte[] e = email.getBytes(StandardCharsets.UTF_8);
        byte[] k = id.getBytes(StandardCharsets.UTF_8);

        byte[] out = new byte[e.length + 1 + k.length];
        System.arraycopy(e, 0, out, 0, e.length);
        out[e.length] = 0x00;
        System.arraycopy(k, 0, out, e.length + 1, k.length);
        return out;
    }
}
