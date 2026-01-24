// File: src/main/java/org/github/dbjo/meta/jdbc/Jdbc.java
package org.github.dbjo.meta.jdbc;

import java.sql.*;

public final class Jdbc {
    private Jdbc() {}

    public static void bind(PreparedStatement ps, Object[] params, SQLType[] types) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object v = params[i];
            if (v == null) {
                int t = types[i].getVendorTypeNumber();
                ps.setNull(i + 1, t);
            } else {
                ps.setObject(i + 1, v);
            }
        }
    }

    public static Short rsShort(ResultSet rs, int i) throws SQLException {
        short v = rs.getShort(i);
        return rs.wasNull() ? null : v;
    }
    public static Integer rsInt(ResultSet rs, int i) throws SQLException {
        int v = rs.getInt(i);
        return rs.wasNull() ? null : v;
    }
    public static Long rsLong(ResultSet rs, int i) throws SQLException {
        long v = rs.getLong(i);
        return rs.wasNull() ? null : v;
    }
    public static Float rsFloat(ResultSet rs, int i) throws SQLException {
        float v = rs.getFloat(i);
        return rs.wasNull() ? null : v;
    }
    public static Double rsDouble(ResultSet rs, int i) throws SQLException {
        double v = rs.getDouble(i);
        return rs.wasNull() ? null : v;
    }
    public static Boolean rsBool(ResultSet rs, int i) throws SQLException {
        boolean v = rs.getBoolean(i);
        return rs.wasNull() ? null : v;
    }
}
