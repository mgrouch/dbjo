package org.github.dbjo.meta.jdbc;

public final class JdbcCrud {
    public static <T> void insert(java.sql.Connection c, DbMeta<T> m, T e) throws Exception {
        try (var ps = c.prepareStatement(m.insertSql())) {
            Jdbc.bind(ps, m.insertParams(e), m.insertParamTypes());
            ps.executeUpdate();
        }
    }

    public static <T> void updateById(java.sql.Connection c, DbMeta<T> m, T e) throws Exception {
        try (var ps = c.prepareStatement(m.updateByIdSql())) {
            Jdbc.bind(ps, m.updateByIdParams(e), m.updateByIdParamTypes());
            ps.executeUpdate();
        }
    }

    public static <T> java.util.List<T> selectAll(java.sql.Connection c, DbMeta<T> m) throws Exception {
        try (var ps = c.prepareStatement(m.selectAllSql());
             var rs = ps.executeQuery()) {
            var out = new java.util.ArrayList<T>();
            while (rs.next()) out.add(m.fromRow(rs));
            return out;
        }
    }
}
