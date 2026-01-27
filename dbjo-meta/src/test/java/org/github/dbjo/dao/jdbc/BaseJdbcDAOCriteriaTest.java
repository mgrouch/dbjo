package org.github.dbjo.dao.jdbc;

import org.github.dbjo.criteria.*;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.github.dbjo.meta.jdbc.DbDialect;
import org.github.dbjo.meta.jdbc.DbMeta;
import org.github.dbjo.meta.jdbc.Jdbc;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.*;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BaseJdbcDAOCriteriaTest {

    public static final class Foo implements Serializable {
        private Integer id;
        private String name;

        public Integer getId() { return id; }
        public void setId(Integer id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }

    private static final PropertyMeta<Foo, Integer> ID = new PropertyMeta<>(
            "id",
            Integer.class,
            Foo::getId,
            Foo::setId
    );

    private static final PropertyMeta<Foo, String> NAME = new PropertyMeta<>(
            "name",
            String.class,
            Foo::getName,
            Foo::setName
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

        @Override public String insertSql() { return "INSERT INTO foo (id, name) VALUES (?, ?)"; }
        @Override public String updateByIdSql() { return "UPDATE foo SET name=? WHERE id=?"; }
        @Override public String selectAllSql() { return "SELECT id, name FROM foo"; }

        @Override public Object[] insertParams(Foo e) { return new Object[]{ e.getId(), e.getName() }; }
        @Override public SQLType[] insertParamTypes() { return null; } // Jdbc.bind can infer with null
        @Override public Object[] updateByIdParams(Foo e) { return new Object[]{ e.getName(), e.getId() }; }
        @Override public SQLType[] updateByIdParamTypes() { return null; }

        // upsert not used in this test
        @Override public String upsertByIdSql(DbDialect dialect) { return "/*unused*/"; }
        @Override public Object[] upsertByIdParams(Foo e) { return new Object[0]; }
        @Override public SQLType[] upsertByIdParamTypes() { return new SQLType[0]; }

        @Override public Foo fromRow(ResultSet rs) throws SQLException {
            Foo f = new Foo();
            f.setId(Jdbc.rsInt(rs, 1));
            f.setName(rs.getString(2));
            return f;
        }
    };

    static final class TestDao extends BaseJdbcDAO<Foo, Integer> {
        TestDao(DataSource ds) {
            super(ds, DbDialect.HSQL, STUB_META);
        }
    }

    @Test
    void daoSelectCriteria_filtersAndHonorsLimit() throws Exception {
        DataSource ds = hsqlMemDs("criteria_test");

        // create schema + seed
        try (Connection c = ds.getConnection()) {
            try (Statement st = c.createStatement()) {
                st.execute("DROP TABLE foo IF EXISTS");
                st.execute("CREATE TABLE foo (id INT PRIMARY KEY, name VARCHAR(50))");
                st.execute("INSERT INTO foo(id,name) VALUES (1,'a'), (2,'ok'), (3,'ok'), (4,'b')");
            }
        }

        var dao = new TestDao(ds);

        var q = Query.from(META)
                .scan(ID, Range.closedOpen(2, 4)) // 2 <= id < 4
                .where(Conditions.eq(NAME, "ok"))
                .limit(1)
                .build();

        List<Foo> got = dao.select(q);

        assertEquals(1, got.size());
        assertEquals(2, got.get(0).getId());
        assertEquals("ok", got.get(0).getName());
    }

    private static DataSource hsqlMemDs(String dbName) {
        // Minimal DS without extra deps; uses DriverManager under the hood.
        return new DataSource() {
            @Override public Connection getConnection() throws SQLException {
                // keep DB alive for the duration of the JVM
                return DriverManager.getConnection("jdbc:hsqldb:mem:" + dbName + ";shutdown=false", "SA", "");
            }
            @Override public Connection getConnection(String username, String password) throws SQLException {
                return DriverManager.getConnection("jdbc:hsqldb:mem:" + dbName + ";shutdown=false", username, password);
            }
            @Override public <T> T unwrap(Class<T> iface) { throw new UnsupportedOperationException(); }
            @Override public boolean isWrapperFor(Class<?> iface) { return false; }
            @Override public java.io.PrintWriter getLogWriter() { return null; }
            @Override public void setLogWriter(java.io.PrintWriter out) {}
            @Override public void setLoginTimeout(int seconds) {}
            @Override public int getLoginTimeout() { return 0; }
            @Override public java.util.logging.Logger getParentLogger() { return java.util.logging.Logger.getGlobal(); }
        };
    }
}
