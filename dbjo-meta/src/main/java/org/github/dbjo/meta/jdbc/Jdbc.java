package org.github.dbjo.meta.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;
import java.util.Objects;

/**
 * Shared JDBC helpers used by generated *DbMeta classes.
 */
public final class Jdbc {
    private Jdbc() {}

    public static void bind(PreparedStatement ps, Object[] params, SQLType[] types) throws SQLException {
        Objects.requireNonNull(ps, "ps");
        if (params == null) params = new Object[0];
        if (types == null) types = new SQLType[0];

        if (types.length != 0 && types.length != params.length) {
            throw new IllegalArgumentException("params.length != types.length: " + params.length + " vs " + types.length);
        }

        for (int i = 0; i < params.length; i++) {
            Object v = params[i];
            int jdbcType = Types.OTHER;

            if (types.length == params.length) {
                SQLType t = types[i];
                Integer vn = (t == null) ? null : t.getVendorTypeNumber();
                if (vn != null) jdbcType = vn;
            }

            if (v == null) {
                ps.setNull(i + 1, jdbcType);
            } else {
                // Prefer typed setObject when we know the SQL type
                if (jdbcType != Types.OTHER) ps.setObject(i + 1, v, jdbcType);
                else ps.setObject(i + 1, v);
            }
        }
    }

    // --- ResultSet nullable primitive wrappers (use only when target Java type is boxed) ---

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
