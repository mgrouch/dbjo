package org.github.dbjo.rdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class DaoSpliteratorIndexEqIT {

    @TempDir
    Path dir;

    @Test
    void indexEqScanWorksWhenIndexValueContainsZeroBytes() throws Exception {
        final String PRIMARY = "things";
        final String IDX = "things_by_val";

        try (RocksDbHandle h = RocksDbBootstrap.open(
                new RocksProps(dir.toString()),
                List.of(RocksSchema.of(PRIMARY, IDX))
        )) {
            RocksSessions sessions = new SpringRocksSessions(h.db());

            Codec<byte[]> bytesCodec = new Codec<>() {
                @Override public byte[] encode(byte[] value) { return value; }
                @Override public byte[] decode(byte[] bytes) { return bytes; }
            };

            IndexDef<byte[], byte[]> byVal = IndexDef.unique(IDX, IndexKeyCodec.rawBytes(), v -> v);

            TestDao dao = new TestDao(
                    sessions,
                    h.cf(PRIMARY),
                    KeyCodecs.int64(),
                    bytesCodec,
                    Map.of(IDX, h.cf(IDX)),
                    List.of(byVal)
            );

            byte[] v = new byte[] { 0x12, 0x00, (byte) 0xAB };  // contains 0x00
            byte[] v2 = new byte[] { 0x12, 0x00, (byte) 0xAC };

            dao.upsert(1L, v);
            dao.upsert(2L, v);
            dao.upsert(3L, v2);

            Query<Long> q = Query.<Long>builder()
                    .where(new IndexPredicate.Eq(IDX, v))
                    .build();

            List<Long> keys;
            try (var s = dao.stream(q)) {
                keys = s.map(Map.Entry::getKey).collect(Collectors.toList());
            }

            assertEquals(List.of(1L, 2L), keys, "expected to find both rows by index value");
        }
    }

    private static final class TestDao extends IndexedRocksDao<byte[], Long> {
        TestDao(RocksSessions sessions,
                org.rocksdb.ColumnFamilyHandle primaryCf,
                KeyCodec<Long> keyCodec,
                Codec<byte[]> valueCodec,
                Map<String, org.rocksdb.ColumnFamilyHandle> indexCfs,
                List<? extends IndexDef<byte[], ?>> indexes) {
            super(sessions, primaryCf, keyCodec, valueCodec, indexCfs, indexes);
        }
    }
}
