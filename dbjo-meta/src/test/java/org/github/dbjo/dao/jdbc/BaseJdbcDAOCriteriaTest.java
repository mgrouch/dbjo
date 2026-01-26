package org.github.dbjo.dao.jdbc;

import org.github.dbjo.criteria.*;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.github.dbjo.meta.jdbc.DbDialect;
import org.github.dbjo.meta.jdbc.DbMeta;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLType;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

public class BaseJdbcDAOCriteriaTest {

    record Foo(Integer id, String name) implements Serializable {}

    private static final PropertyMeta<Foo, Integer> ID = new PropertyMeta<>(
            "id",
            Integer.class,
            Foo::id,
            (b, v) -> { /* unused */ }
    );

    private static final PropertyMeta<Foo, String> NAME = new PropertyMeta<>(
            "name",
            String.class,
            Foo::name,
            (b, v) -> { /* unused */ }
    );

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final EntityMeta<Foo> META = new EntityMeta<>(
            (List) List.of(ID, NAME),
            List.of("id", "name"),
            List.of(Integer.class, String.class)
    );

    private static final DbMeta<Foo> STUB_META = new DbMeta<>() {
        @Override public String schema() { return null; }
        @Override public String table()  { return "foo"; }
        @Override public String fqn()    { return "foo"; }

        @Override public String insertSql() { return "/*unused*/"; }
        @Override public String updateByIdSql() { return "/*unused*/"; }
        @Override public String selectAllSql() { return "/*unused*/"; }

        @Override public Object[] insertParams(Foo e) { return new Object[0]; }
        @Override public SQLType[] insertParamTypes() { return new SQLType[0]; }

        @Override public Object[] updateByIdParams(Foo e) { return new Object[0]; }
        @Override public SQLType[] updateByIdParamTypes() { return new SQLType[0]; }

        @Override public String upsertByIdSql(DbDialect dialect) { return "/*unused*/"; }
        @Override public Object[] upsertByIdParams(Foo e) { return new Object[0]; }
        @Override public SQLType[] upsertByIdParamTypes() { return new SQLType[0]; }

        @Override public Foo fromRow(ResultSet rs) { throw new UnsupportedOperationException(); }
    };

    static final class TestDao extends BaseJdbcDAO<Foo, Integer> {
        TestDao(DataSource ds) {
            super(ds, DbDialect.HSQL, STUB_META);
        }

        @Override
        public List<Foo> selectAll(Connection c) {
            // No DB access: just returns a stable in-memory table.
            return List.of(
                    new Foo(1, "a"),
                    new Foo(2, "ok"),
                    new Foo(3, "ok"),
                    new Foo(4, "b")
            );
        }
    }

    @Test
    void daoSelectCriteria_filtersAndHonorsLimit() throws SQLException {
        DataSource ds = dummyDataSource();
        var dao = new TestDao(ds);

        var q = Query.from(META)
                .scan(ID, Range.closedOpen(2, 4))     // id in [2,4)
                .where(Conditions.eq(NAME, "ok"))      // name == "ok"
                .limit(1)
                .build();

        try (Connection c = dummyConnection()) {
            var got = dao.select(c, q);
            assertEquals(1, got.size());
            assertEquals(2, got.get(0).id());
            assertEquals("ok", got.get(0).name());
        }
    }

    private static DataSource dummyDataSource() {
        return new DataSource() {
            @Override public Connection getConnection() { throw new UnsupportedOperationException(); }
            @Override public Connection getConnection(String username, String password) { throw new UnsupportedOperationException(); }
            @Override public <T> T unwrap(Class<T> iface) { throw new UnsupportedOperationException(); }
            @Override public boolean isWrapperFor(Class<?> iface) { return false; }
            @Override public PrintWriter getLogWriter() { return null; }
            @Override public void setLogWriter(PrintWriter out) {}
            @Override public void setLoginTimeout(int seconds) {}
            @Override public int getLoginTimeout() { return 0; }
            @Override public Logger getParentLogger() { return Logger.getGlobal(); }
        };
    }

    private static Connection dummyConnection() {
        return (Connection) Proxy.newProxyInstance(
                BaseJdbcDAOCriteriaTest.class.getClassLoader(),
                new Class[]{Connection.class},
                (p, m, a) -> {
                    if (Objects.equals(m.getName(), "close")) return null;
                    // This test should not call any Connection methods.
                    throw new UnsupportedOperationException(m.getName());
                }
        );
    }
}
