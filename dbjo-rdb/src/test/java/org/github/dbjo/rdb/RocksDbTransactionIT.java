package org.github.dbjo.rdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RocksDbTransactionIT {

    @TempDir
    Path dir;

    @Test
    void commitAndRollbackWithSpringTransactionManager() throws Exception {
        RocksProps props = new RocksProps(dir.resolve("rocks").toString());

        try (RocksDbHandle handle = RocksDbBootstrap.open(props, List.of(RocksSchema.of("person")))) {
            RocksDbTransactionManager txManager = new RocksDbTransactionManager(handle.db());
            TransactionTemplate template = new TransactionTemplate(txManager);
            SpringRocksSessions sessions = new SpringRocksSessions(handle.db());

            PersonDao dao = new PersonDao(sessions, handle.cf("person"));

            template.executeWithoutResult(status -> dao.upsert(1, "alpha"));
            assertThat(dao.findByKey(1)).contains("alpha");

            template.executeWithoutResult(status -> {
                dao.upsert(2, "beta");
                status.setRollbackOnly();
            });
            assertThat(dao.findByKey(2)).isEmpty();
        }
    }

    private static final class PersonDao extends AbstractRocksDao<String, Integer> {
        private PersonDao(RocksSessions sessions, org.rocksdb.ColumnFamilyHandle primaryCf) {
            super(sessions, primaryCf, KeyCodec.int32(), new StringCodec(), Map.of());
        }

        @Override
        protected void maintainIndexes(RocksWriteBatch batch, Integer key, String oldValueOrNull, String newValue) {
        }

        @Override
        protected void maintainIndexesOnDelete(RocksWriteBatch batch, Integer key, String oldValue) {
        }
    }

    private static final class StringCodec implements Codec<String> {
        @Override
        public byte[] encode(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
