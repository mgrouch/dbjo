package org.github.dbjo.criteria.sql;

import org.github.dbjo.criteria.*;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.github.dbjo.meta.jdbc.DbDialect;
import org.github.dbjo.meta.jdbc.DbMeta;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLType;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Drop-in unit tests for {@link SqlCriteriaCompiler}.
 *
 * These tests only validate SQL/params compilation; no DB is required.
 */
final class SqlCriteriaCompilerTest {

    record Foo(Integer id, String createdAt) implements Serializable {}

    private static final PropertyMeta<Foo, Integer> ID =
            new PropertyMeta<>("id", Integer.class, Foo::id, (b, v) -> { /* unused */ });

    private static final PropertyMeta<Foo, String> CREATED_AT =
            new PropertyMeta<>("createdAt", String.class, Foo::createdAt, (b, v) -> { /* unused */ });

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final EntityMeta<Foo> META = new EntityMeta(
            java.util.List.of(ID, CREATED_AT),
            java.util.List.of("id", "createdAt"),
            java.util.List.of(Integer.class, String.class)
    );

    private static final DbMeta<Foo> DB_META = new DbMeta<>() {
        @Override public String schema() { return "PUBLIC"; }
        @Override public String table()  { return "FOO"; }
        @Override public String fqn()    { return "PUBLIC.FOO"; }

        @Override public String selectAllSql() { return "SELECT * FROM foo;"; }
        @Override public String insertSql() { return ""; }

        @Override public Object[] insertParams(Foo e) { return new Object[0]; }
        @Override public SQLType[] insertParamTypes() { return new SQLType[0]; }

        @Override public String updateByIdSql() { return ""; }
        @Override public Object[] updateByIdParams(Foo e) { return new Object[0]; }
        @Override public SQLType[] updateByIdParamTypes() { return new SQLType[0]; }

        @Override public String upsertByIdSql(DbDialect dialect) { return ""; }
        @Override public Object[] upsertByIdParams(Foo e) { return new Object[0]; }
        @Override public SQLType[] upsertByIdParamTypes() { return new SQLType[0]; }

        @Override public Foo fromRow(ResultSet rs) { throw new UnsupportedOperationException("not needed"); }
    };

    @Test
    void defaultQuery_hasNoWhereClause() {
        org.github.dbjo.criteria.Query<Foo> q = org.github.dbjo.criteria.Query.from(META).build();

        SqlCriteriaCompiler.Compiled out = SqlCriteriaCompiler.compileSelectAll(DB_META, q);

        assertEquals("SELECT * FROM foo", out.sql());
        assertArrayEquals(new Object[0], out.params());
    }

    @Test
    void falseWhere_becomesWhere1eq0() {
        org.github.dbjo.criteria.Query<Foo> q = org.github.dbjo.criteria.Query.from(META)
                .where(Conditions.falseCondition())
                .build();

        SqlCriteriaCompiler.Compiled out = SqlCriteriaCompiler.compileSelectAll(DB_META, q);

        assertEquals("SELECT * FROM foo WHERE 1=0", out.sql());
        assertArrayEquals(new Object[0], out.params());
    }

    @Test
    void eqNull_compilesToIsNullWithoutParams() {
        org.github.dbjo.criteria.Query<Foo> q = org.github.dbjo.criteria.Query.from(META)
                .where(Conditions.eq(CREATED_AT, null))
                .build();

        SqlCriteriaCompiler.Compiled out = SqlCriteriaCompiler.compileSelectAll(DB_META, q);

        assertEquals("SELECT * FROM foo WHERE created_at IS NULL", out.sql());
        assertArrayEquals(new Object[0], out.params());
    }

    @Test
    void notTrue_isTreatedAsNoCondition() {
        org.github.dbjo.criteria.Query<Foo> q = org.github.dbjo.criteria.Query.from(META)
                .where(Conditions.<Foo>trueCondition().not())
                .build();

        SqlCriteriaCompiler.Compiled out = SqlCriteriaCompiler.compileSelectAll(DB_META, q);

        assertEquals("SELECT * FROM foo", out.sql());
        assertArrayEquals(new Object[0], out.params());
    }

    @Test
    void scanRangeClosedOpen_compilesToGeAndLt() {
        org.github.dbjo.criteria.Query<Foo> q = org.github.dbjo.criteria.Query.from(META)
                .scan(ID, Range.closedOpen(2, 4))
                .build();

        SqlCriteriaCompiler.Compiled out = SqlCriteriaCompiler.compileSelectAll(DB_META, q);

        assertEquals("SELECT * FROM foo WHERE (id >= ?) AND (id < ?)", out.sql());
        assertArrayEquals(new Object[]{2, 4}, out.params());
    }

    @Test
    void whereAndScan_areCombinedWithAnd() {
        org.github.dbjo.criteria.Query<Foo> q = org.github.dbjo.criteria.Query.from(META)
                .scan(ID, Range.atLeast(10))
                .where(Conditions.eq(CREATED_AT, "x"))
                .build();

        SqlCriteriaCompiler.Compiled out = SqlCriteriaCompiler.compileSelectAll(DB_META, q);

        assertEquals("SELECT * FROM foo WHERE (id >= ?) AND (created_at = ?)", out.sql());
        assertArrayEquals(new Object[]{10, "x"}, out.params());
    }

    @Test
    void like_compilesToLikeWithParam() {
        org.github.dbjo.criteria.Query<Foo> q = org.github.dbjo.criteria.Query.from(META)
                .where(Conditions.like(CREATED_AT, "%2024%"))
                .build();

        SqlCriteriaCompiler.Compiled out = SqlCriteriaCompiler.compileSelectAll(DB_META, q);

        assertEquals("SELECT * FROM foo WHERE created_at LIKE ?", out.sql());
        assertArrayEquals(new Object[]{"%2024%"}, out.params());
    }

    @Test
    void orderByAsc_compilesOrderByClause() {
        org.github.dbjo.criteria.Query<Foo> q = org.github.dbjo.criteria.Query.from(META)
                .orderByAsc(ID)
                .build();

        SqlCriteriaCompiler.Compiled out = SqlCriteriaCompiler.compileSelectAll(DB_META, q);

        assertEquals("SELECT * FROM foo ORDER BY id ASC", out.sql());
        assertArrayEquals(new Object[0], out.params());
    }

    @Test
    void orderByMulti_withWhere_compilesOrderByAfterWhere() {
        org.github.dbjo.criteria.Query<Foo> q = org.github.dbjo.criteria.Query.from(META)
                .where(Conditions.eq(ID, 7))
                .orderByDesc(CREATED_AT)
                .orderByAsc(ID)
                .build();

        SqlCriteriaCompiler.Compiled out = SqlCriteriaCompiler.compileSelectAll(DB_META, q);

        assertEquals("SELECT * FROM foo WHERE id = ? ORDER BY created_at DESC, id ASC", out.sql());
        assertArrayEquals(new Object[]{7}, out.params());
    }
}
