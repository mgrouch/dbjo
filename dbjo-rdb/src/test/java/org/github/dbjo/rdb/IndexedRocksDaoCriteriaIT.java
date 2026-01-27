package org.github.dbjo.rdb;

import org.github.dbjo.criteria.Conditions;
import org.github.dbjo.criteria.Query;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class IndexedRocksDaoCriteriaIT {

    static { RocksDB.loadLibrary(); }

    @TempDir
    Path dir;

    // ---- test entity ----

    public static final class Person implements Serializable {
        private int id;
        private String region;
        private int age;

        public Person() {}
        public Person(int id, String region, int age) {
            this.id = id;
            this.region = region;
            this.age = age;
        }

        public int getId() { return id; }
        public void setId(int id) { this.id = id; }

        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }

        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
    }

    // PropertyMeta signature: (name, type, getter, setter)
    private static final PropertyMeta<Person, Integer> P_ID =
            new PropertyMeta<>("id", Integer.class, Person::getId, (p, v) -> p.setId(v == null ? 0 : v));

    private static final PropertyMeta<Person, String> P_REGION =
            new PropertyMeta<>("region", String.class, Person::getRegion, Person::setRegion);

    private static final PropertyMeta<Person, Integer> P_AGE =
            new PropertyMeta<>("age", Integer.class, Person::getAge, (p, v) -> p.setAge(v == null ? 0 : v));

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final EntityMeta<Person> PERSON_META = new EntityMeta(
            (List) List.of(P_ID, P_REGION, P_AGE),
            List.of("id", "region", "age"),
            List.of(Integer.class, String.class, Integer.class)
    );

    // ---- codecs ----

    private static final class PersonCodec implements Codec<Person> {
        @Override
        public byte[] encode(Person value) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeInt(value.getId());
                dos.writeInt(value.getAge());

                byte[] r = value.getRegion() == null ? new byte[0] : value.getRegion().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(r.length);
                dos.write(r);

                dos.flush();
                return baos.toByteArray();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public Person decode(byte[] bytes) {
            try {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
                int id = dis.readInt();
                int age = dis.readInt();
                int n = dis.readInt();
                byte[] r = dis.readNBytes(n);
                String region = n == 0 ? null : new String(r, StandardCharsets.UTF_8);
                return new Person(id, region, age);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static final class CountingCodec<T> implements Codec<T> {
        private final Codec<T> inner;
        private final AtomicInteger decodes;

        private CountingCodec(Codec<T> inner, AtomicInteger decodes) {
            this.inner = inner;
            this.decodes = decodes;
        }

        @Override public byte[] encode(T value) { return inner.encode(value); }

        @Override public T decode(byte[] bytes) {
            decodes.incrementAndGet();
            return inner.decode(bytes);
        }
    }

    // order-preserving int32 encoding for index value bytes (same style as KeyCodecs.int32)
    private static final IndexKeyCodec<Integer> INT32_ORDERED = v -> {
        if (v == null) return null;
        int x = v ^ 0x80000000; // flip sign bit to preserve signed ordering lexicographically
        return ByteBuffer.allocate(4).putInt(x).array();
    };

    // ---- minimal sessions (auto-commit, no Spring) ----

    private static final class SimpleSessions implements RocksSessions {
        private final TransactionDB db;
        SimpleSessions(TransactionDB db) { this.db = db; }

        @Override
        public RocksSession current() {
            return new RocksSession() {
                @Override public ReadOptions newReadOptions() { return new ReadOptions(); }

                @Override
                public byte[] get(ColumnFamilyHandle cf, ReadOptions ro, byte[] key) throws RocksDBException {
                    return db.get(cf, ro, key);
                }

                @Override
                public RocksIterator iterator(ColumnFamilyHandle cf, ReadOptions ro) {
                    return db.newIterator(cf, ro);
                }

                @Override
                public void write(RocksWriteBatch batch) throws RocksDBException {
                    if (batch.isEmpty()) return;
                    try (WriteOptions wo = new WriteOptions(); WriteBatch wb = new WriteBatch()) {
                        for (var op : batch.ops()) {
                            if (op instanceof RocksWriteBatch.Put p) wb.put(p.cf(), p.key(), p.value());
                            else if (op instanceof RocksWriteBatch.Delete d) wb.delete(d.cf(), d.key());
                        }
                        db.write(wo, wb);
                    }
                }
            };
        }
    }

    // ---- dao ----

    private static final class PersonDao extends IndexedRocksDao<Person, Integer> {
        PersonDao(RocksSessions sessions,
                  ColumnFamilyHandle primary,
                  Codec<Person> valueCodec,
                  Map<String, ColumnFamilyHandle> indexCfs) {
            super(
                    sessions,
                    primary,
                    KeyCodec.int32(),
                    valueCodec,
                    indexCfs,
                    List.of(
                            // keep your IndexDef API as-is (propertyName overload etc.) if you already added it
                            IndexDef.unique("idx_region", "region", IndexKeyCodec.stringUtf8(), Person::getRegion),
                            IndexDef.unique("idx_age",    "age",    INT32_ORDERED, Person::getAge)
                    )
            );
        }
    }

    private RocksDbHandle openDb() throws RocksDBException {
        RocksProps props = new RocksProps(dir.resolve("rocks").toString());
        return RocksDbBootstrap.open(props, List.of(RocksSchema.of("person", "idx_region", "idx_age")));
    }

    private static void populate(PersonDao dao, int n) {
        for (int i = 1; i <= n; i++) {
            String region = (i % 997 == 0) ? "Z" : (i % 983 == 0) ? "Y" : "A";
            int age = i % 100;
            dao.upsert(i, new Person(i, region, age));
        }
    }

    @Test
    void eq_on_indexed_property_pushes_down_to_index_scan() throws Exception {
        try (RocksDbHandle h = openDb()) {
            AtomicInteger decodes = new AtomicInteger();
            Codec<Person> codec = new CountingCodec<>(new PersonCodec(), decodes);

            TransactionDB db = h.db();
            PersonDao dao = new PersonDao(
                    new SimpleSessions(db),
                    h.cf("person"),
                    codec,
                    Map.of("idx_region", h.cf("idx_region"), "idx_age", h.cf("idx_age"))
            );

            populate(dao, 2000);
            decodes.set(0);

            Query<Person> q = Query.from(PERSON_META)
                    .where(Conditions.eq(P_REGION, "Z"))
                    .build();

            List<Person> res = dao.select(q);

            assertThat(res).isNotEmpty();
            assertThat(res).allMatch(p -> "Z".equals(p.getRegion()));
            assertThat(decodes.get()).isLessThan(100);
        }
    }

    @Test
    void between_on_indexed_property_pushes_down_to_index_range_scan() throws Exception {
        try (RocksDbHandle h = openDb()) {
            AtomicInteger decodes = new AtomicInteger();
            Codec<Person> codec = new CountingCodec<>(new PersonCodec(), decodes);

            TransactionDB db = h.db();
            PersonDao dao = new PersonDao(
                    new SimpleSessions(db),
                    h.cf("person"),
                    codec,
                    Map.of("idx_region", h.cf("idx_region"), "idx_age", h.cf("idx_age"))
            );

            populate(dao, 2000);
            decodes.set(0);

            Query<Person> q = Query.from(PERSON_META)
                    .where(Conditions.between(P_AGE, 10, 12))
                    .build();

            List<Person> res = dao.select(q);

            assertThat(res).isNotEmpty();
            assertThat(res).allMatch(p -> p.getAge() >= 10 && p.getAge() <= 12);
            assertThat(decodes.get()).isLessThan(300);
        }
    }

    @Test
    void in_on_indexed_property_pushes_down_as_union_of_eq_scans() throws Exception {
        try (RocksDbHandle h = openDb()) {
            AtomicInteger decodes = new AtomicInteger();
            Codec<Person> codec = new CountingCodec<>(new PersonCodec(), decodes);

            TransactionDB db = h.db();
            PersonDao dao = new PersonDao(
                    new SimpleSessions(db),
                    h.cf("person"),
                    codec,
                    Map.of("idx_region", h.cf("idx_region"), "idx_age", h.cf("idx_age"))
            );

            populate(dao, 2000);
            decodes.set(0);

            Query<Person> q = Query.from(PERSON_META)
                    .where(Conditions.in(P_REGION, List.of("Z", "Y")))
                    .build();

            List<Person> res = dao.select(q);

            assertThat(res).isNotEmpty();
            assertThat(res).allMatch(p -> "Z".equals(p.getRegion()) || "Y".equals(p.getRegion()));
            assertThat(decodes.get()).isLessThan(200);
        }
    }
}
