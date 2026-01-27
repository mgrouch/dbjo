package org.github.dbjo.meta.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLType;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Drop-in tests for {@link DbMeta#columnOf(String)} naming rules.
 *
 * NOTE: in dbjo-0.1.7 DbMeta#columnOf is an instance method.
 */
final class DbMetaColumnOfTest {

    private static final DbMeta<Object> META = new DbMeta<>() {
        @Override public String schema() { return "PUBLIC"; }
        @Override public String table()  { return "T"; }
        @Override public String fqn()    { return "PUBLIC.T"; }

        @Override public String selectAllSql() { return "SELECT 1"; }
        @Override public String insertSql() { return ""; }

        @Override public Object[] insertParams(Object bean) { return new Object[0]; }
        @Override public SQLType[] insertParamTypes() { return new SQLType[0]; }

        @Override public String updateByIdSql() { return ""; }
        @Override public Object[] updateByIdParams(Object e) { return new Object[0]; }
        @Override public SQLType[] updateByIdParamTypes() { return new SQLType[0]; }

        @Override public String upsertByIdSql(DbDialect dialect) { return ""; }
        @Override public Object[] upsertByIdParams(Object e) { return new Object[0]; }
        @Override public SQLType[] upsertByIdParamTypes() { return new SQLType[0]; }

        @Override public Object fromRow(ResultSet rs) { throw new UnsupportedOperationException("not needed"); }
    };

    @Test
    void camelCase_to_snake_case() {
        assertEquals("created_at", META.columnOf("createdAt"));
        assertEquals("user_id", META.columnOf("userId"));
        assertEquals("a_b", META.columnOf("aB"));
        assertEquals("a1_b", META.columnOf("a1B"));
        assertEquals("url", META.columnOf("URL"));
        assertEquals("user_id", META.columnOf("userID"));
    }

    @Test
    void already_snake_case_is_preserved() {
        assertEquals("created_at", META.columnOf("created_at"));
        assertEquals("x", META.columnOf("x"));
    }
}
