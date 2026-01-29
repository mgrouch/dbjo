package org.github.dbjo.rdb;

import org.github.dbjo.criteria.*;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyHandle;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for criteria pushdown planning in {@link IndexedRocksDao}.
 *
 * These tests do NOT open RocksDB. Instead, they override {@link IndexedRocksDao#stream(Query)}
 * to capture planned Rocks {@link Query} objects.
 */
final class IndexedRocksDaoCriteriaPlanTest {

    record Foo(Integer id, Integer age, String name) implements Serializable {}

    private static final PropertyMeta<Foo, Integer> ID =
            new PropertyMeta<>("id", Integer.class, Foo::id, (b, v) -> {});

    private static final PropertyMeta<Foo, Integer> AGE =
            new PropertyMeta<>("age", Integer.class, Foo::age, (b, v) -> {});

    private static final PropertyMeta<Foo, String> NAME =
            new PropertyMeta<>("name", String.class, Foo::name, (b, v) -> {});

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final EntityMeta<Foo> META = new EntityMeta(
            List.of(ID, AGE, NAME),
            List.of("id", "age", "name"),
            List.of(Integer.class, Integer.class, String.class)
    );

    private static final IndexDef<Foo, Integer> IDX_ID =
            IndexDef.unique("idx_id", "id", IndexKeyCodec.int32(), Foo::id);

    private static final IndexDef<Foo, Integer> IDX_AGE =
            IndexDef.unique("idx_age", "age", IndexKeyCodec.int32(), Foo::age);

    private static final IndexDef<Foo, String> IDX_NAME =
            IndexDef.unique("idx_name", "name", IndexKeyCodec.stringUtf8(), Foo::name);

    private static TestDao newDao() {
        RocksSessions sessions = () -> { throw new AssertionError("sessions.current() should not be called"); };

        ColumnFamilyHandle primaryCf = mock(ColumnFamilyHandle.class);

        Map<String, ColumnFamilyHandle> indexCfs = Map.of(
                IDX_ID.name(), mock(ColumnFamilyHandle.class),
                IDX_AGE.name(), mock(ColumnFamilyHandle.class),
                IDX_NAME.name(), mock(ColumnFamilyHandle.class)
        );

        Codec<Foo> valueCodec = new Codec<>() {
            @Override public byte[] encode(Foo value) { throw new UnsupportedOperationException(); }
            @Override public Foo decode(byte[] bytes) { throw new UnsupportedOperationException(); }
        };

        return new TestDao(
                sessions,
                primaryCf,
                KeyCodecs.int32(),
                valueCodec,
                indexCfs,
                List.of(IDX_ID, IDX_AGE, IDX_NAME)
        );
    }

    final TestDao dao = newDao();

    private static final class TestDao extends IndexedRocksDao<Foo, Integer> {
        final List<Query<Integer>> seen = new ArrayList<>();

        TestDao(RocksSessions sessions,
                ColumnFamilyHandle primaryCf,
                KeyCodec<Integer> keyCodec,
                Codec<Foo> valueCodec,
                Map<String, ColumnFamilyHandle> indexCfs,
                List<IndexDef<Foo, ?>> indexes) {
            super(sessions, primaryCf, keyCodec, valueCodec, indexCfs, indexes);
        }

        @Override
        public Stream<Map.Entry<Integer, Foo>> stream(Query<Integer> q) {
            seen.add(q);
            return Stream.empty();
        }
    }

    // helpers
    private static byte[] i32(int v) { return IndexKeyCodec.int32().encode(v); }
    private static byte[] utf8(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    private static void assertNoIndex(Query<Integer> q) {
        assertTrue(q.indexPredicates().isEmpty(), "expected no index predicate");
    }

    private static void assertEq(Query<Integer> q, String idx, byte[] expected) {
        assertEquals(1, q.indexPredicates().size());
        assertTrue(q.indexPredicates().get(0) instanceof IndexPredicate.Eq);
        IndexPredicate.Eq p = (IndexPredicate.Eq) q.indexPredicates().get(0);
        assertEquals(idx, p.indexName());
        assertArrayEquals(expected, p.valueBytes());
    }

    private static void assertRange(Query<Integer> q, String idx,
                                    byte[] from,
                                    byte[] to, boolean toIncl) {
        assertEquals(1, q.indexPredicates().size());
        assertTrue(q.indexPredicates().get(0) instanceof IndexPredicate.Range);
        IndexPredicate.Range r = (IndexPredicate.Range) q.indexPredicates().get(0);
        assertEquals(idx, r.indexName());
        assertArrayEquals(from, r.from());
        assertTrue(r.fromInclusive());
        assertArrayEquals(to, r.to());
        assertEquals(toIncl, r.toInclusive());
    }

    @Test
    void eqPushdown_usesMatchingIndex() {
        var cq = org.github.dbjo.criteria.Query.from(META)
                .where(Conditions.eq(AGE, 42))
                .build();

        dao.select(cq);

        assertEquals(1, dao.seen.size());
        assertEq(dao.seen.get(0), "idx_age", i32(42));
    }

    @Test
    void scanPushdown_rangeUsesIndex() {
        TestDao dao = newDao();

        var cq = org.github.dbjo.criteria.Query.from(META)
                .scan(ID, Range.closedOpen(10, 20))
                .build();

        dao.select(cq);

        assertEquals(1, dao.seen.size());
        assertRange(dao.seen.get(0), "idx_id", i32(10), i32(20), false);
    }

    @Test
    void whereEq_beatsScan_inPlanner() {
        var cq = org.github.dbjo.criteria.Query.from(META)
                .scan(ID, Range.atLeast(5))
                .where(Conditions.eq(NAME, "bob")) // score 1000 beats scan 700
                .build();

        dao.select(cq);

        assertEquals(1, dao.seen.size());
        assertEq(dao.seen.get(0), "idx_name", utf8("bob"));
    }

    @Test
    void andChoosesBestChildPredicate() {
        Condition<Foo> c = Conditions.eq(NAME, "x")
                .and(Conditions.between(AGE, 1, 2)); // between score 650, eq score 1000

        var cq = org.github.dbjo.criteria.Query.from(META).where(c).build();
        dao.select(cq);

        assertEquals(1, dao.seen.size());
        assertEq(dao.seen.get(0), "idx_name", utf8("x"));
    }

    @Test
    void betweenPushdown_isInclusiveRange() {
        var cq = org.github.dbjo.criteria.Query.from(META)
                .where(Conditions.between(AGE, 5, 7))
                .build();

        dao.select(cq);

        assertEquals(1, dao.seen.size());
        assertRange(dao.seen.get(0), "idx_age", i32(5), i32(7), true);
    }

    @Test
    void inSmall_pushesUnionOfEqQueries() {
        var cq = org.github.dbjo.criteria.Query.from(META)
                .where(Conditions.in(NAME, "a", "b", "c"))
                .limit(10)
                .build();

        dao.select(cq);

        assertEquals(3, dao.seen.size());
        assertEq(dao.seen.get(0), "idx_name", utf8("a"));
        assertEq(dao.seen.get(1), "idx_name", utf8("b"));
        assertEq(dao.seen.get(2), "idx_name", utf8("c"));
    }

    @Test
    void orOfEqSameProp_pushesUnion() {
        Condition<Foo> c = new Or<>(
                new Eq<>(NAME, "a"),
                new Or<>(new Eq<>(NAME, "b"), new Eq<>(NAME, "c"))
        );

        var cq = org.github.dbjo.criteria.Query.from(META).where(c).build();
        dao.select(cq);

        assertEquals(3, dao.seen.size());
        assertEq(dao.seen.get(0), "idx_name", utf8("a"));
        assertEq(dao.seen.get(1), "idx_name", utf8("b"));
        assertEq(dao.seen.get(2), "idx_name", utf8("c"));
    }

    @Test
    void orDifferentProps_doesNotPushdown() {
        Condition<Foo> c = new Or<>(new Eq<>(NAME, "a"), new Eq<>(AGE, 1));
        var cq = org.github.dbjo.criteria.Query.from(META).where(c).build();

        dao.select(cq);

        assertEquals(1, dao.seen.size());
        assertNoIndex(dao.seen.get(0));
    }

    @Test
    void inTooLarge_doesNotPushdown() {
        List<String> vals = new ArrayList<>();
        for (int i = 0; i < 17; i++) vals.add("v" + i); // planner rejects > 16

        Condition<Foo> c = new In<>(NAME, vals);
        var cq = org.github.dbjo.criteria.Query.from(META).where(c).build();

        dao.select(cq);

        assertEquals(1, dao.seen.size());
        assertNoIndex(dao.seen.get(0));
    }

    @Test
    void eqNull_doesNotPushdown() {
        Condition<Foo> c = new Eq<>(NAME, null); // bypass Conditions.eq(null) -> IsNull
        var cq = org.github.dbjo.criteria.Query.from(META).where(c).build();

        dao.select(cq);

        assertEquals(1, dao.seen.size());
        assertNoIndex(dao.seen.get(0));
    }
}
