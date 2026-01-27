package org.github.dbjo.rdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class DaoSpliteratorTest {

    @TempDir
    Path dir;

    private static final Codec<byte[]> BYTES_CODEC = new Codec<>() {
        @Override public byte[] encode(byte[] value) { return value; }
        @Override public byte[] decode(byte[] bytes) { return bytes; }
    };

    private static final IndexDef<byte[], byte[]> BY_VAL =
            IndexDef.unique("by_val", IndexKeyCodec.rawBytes(), v -> v);

    private static final class BytesDao extends IndexedRocksDao<byte[], Long> {
        BytesDao(RocksSessions sessions,
                 org.rocksdb.ColumnFamilyHandle primaryCf,
                 Map<String, org.rocksdb.ColumnFamilyHandle> indexCfs) {
            super(sessions, primaryCf, KeyCodecs.int64(), BYTES_CODEC, indexCfs, List.of(BY_VAL));
        }
    }

    @Test
    void indexEqScanReturnsOnlyMatchingRow() throws Exception {
        RocksProps props = new RocksProps(dir.resolve("db").toString());

        try (RocksDbHandle h = RocksDbBootstrap.open(props, List.of(RocksSchema.of("things", "by_val")))) {
            RocksSessions sessions = new SpringRocksSessions(h.db());
            BytesDao dao = new BytesDao(sessions, h.cf("things"), Map.of("by_val", h.cf("by_val")));

            dao.upsert(1L, "aa".getBytes(StandardCharsets.UTF_8));
            dao.upsert(2L, "bb".getBytes(StandardCharsets.UTF_8));
            dao.upsert(3L, "cc".getBytes(StandardCharsets.UTF_8));

            Query<Long> q = Query.<Long>builder()
                    .where(new IndexPredicate.Eq("by_val", "bb".getBytes(StandardCharsets.UTF_8)))
                    .build();

            var out = dao.stream(q).toList();
            assertEquals(1, out.size());
            assertEquals(2L, out.getFirst().getKey());
            assertArrayEquals("bb".getBytes(StandardCharsets.UTF_8), out.getFirst().getValue());
        }
    }

    @Test
    void indexRangeScanHonorsInclusiveExclusiveBounds() throws Exception {
        RocksProps props = new RocksProps(dir.resolve("db2").toString());

        try (RocksDbHandle h = RocksDbBootstrap.open(props, List.of(RocksSchema.of("things", "by_val")))) {
            RocksSessions sessions = new SpringRocksSessions(h.db());
            BytesDao dao = new BytesDao(sessions, h.cf("things"), Map.of("by_val", h.cf("by_val")));

            dao.upsert(1L, "a".getBytes(StandardCharsets.UTF_8));
            dao.upsert(2L, "b".getBytes(StandardCharsets.UTF_8));
            dao.upsert(3L, "c".getBytes(StandardCharsets.UTF_8));
            dao.upsert(4L, "d".getBytes(StandardCharsets.UTF_8));

            Query<Long> q = Query.<Long>builder()
                    .where(new IndexPredicate.Range(
                            "by_val",
                            "b".getBytes(StandardCharsets.UTF_8), true,   // [b
                            "d".getBytes(StandardCharsets.UTF_8), false   //  d)
                    ))
                    .build();

            var keys = dao.stream(q).map(Map.Entry::getKey).toList();
            assertEquals(List.of(2L, 3L), keys);
        }
    }
}
