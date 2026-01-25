// File: src/main/java/org/github/dbjo/meta/jdbc/Jdbc.java
package org.github.dbjo.meta.jdbc;

import java.sql.*;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.UUID;

public final class Jdbc {
    private Jdbc() {}

    /**
     * Binds params using SQLType hints when it is likely to matter (TZ-aware time types, UUID, etc).
     * Safe across Oracle/MSSQL/Sybase/HSQL.
     */
    public static void bind(PreparedStatement ps, Object[] params, SQLType[] types) throws SQLException {
        if (params == null || params.length == 0) return;

        for (int i = 0; i < params.length; i++) {
            Object v = params[i];
            SQLType t = (types != null && i < types.length) ? types[i] : null;
            Integer vendor = (t == null) ? null : t.getVendorTypeNumber();

            if (v == null) {
                // Prefer vendor type, fall back to Types.NULL
                if (vendor != null) ps.setNull(i + 1, vendor);
                else ps.setNull(i + 1, Types.NULL);
                continue;
            }

            // Some drivers do better with direct setters for common cases.
            if (v instanceof byte[] b) {
                ps.setBytes(i + 1, b);
                continue;
            }

            // Use typed setObject only where it helps and is unlikely to harm.
            if (shouldUseTypedSetObject(v, t)) {
                try {
                    // JDBC 4.2: setObject(index, value, SQLType)
                    ps.setObject(i + 1, v, t);
                    continue;
                } catch (SQLFeatureNotSupportedException ignored) {
                    // fall through to other strategies
                } catch (SQLException e) {
                    // Some drivers are picky about "OTHER" / specific combos; fall back.
                    // Don't rethrow yet; try a less strict variant first.
                }
            }

            // Next-best: typed by vendor int, then untyped.
            if (vendor != null) {
                try {
                    ps.setObject(i + 1, v, vendor);
                    continue;
                } catch (SQLException ignored) {
                    // fall through
                }
            }

            ps.setObject(i + 1, v);
        }
    }

    /**
     * Add one row to a batch with the same binding logic as bind(..).
     */
    public static void addBatch(PreparedStatement ps, Object[] params, SQLType[] types) throws SQLException {
        bind(ps, params, types);
        ps.addBatch();
    }

    /**
     * Helper: sum executeBatch() results (counts may include SUCCESS_NO_INFO).
     */
    public static int sumBatchCounts(int[] counts) {
        if (counts == null) return 0;
        int sum = 0;
        for (int c : counts) {
            if (c == Statement.SUCCESS_NO_INFO) sum += 1;
            else if (c > 0) sum += c;
        }
        return sum;
    }

    private static boolean shouldUseTypedSetObject(Object v, SQLType t) {
        if (t == null) return false;

        // Strongly prefer typed binding for these; drivers differ a lot otherwise.
        if (v instanceof OffsetDateTime) return true;
        if (v instanceof OffsetTime) return true;
        if (v instanceof UUID) return true;

        // For JDBCType hints, only use typed for the time-related / numeric cases where it helps.
        if (t instanceof JDBCType jt) {
            return switch (jt) {
                case DATE,
                        TIME,
                        TIMESTAMP,
                        TIME_WITH_TIMEZONE,
                        TIMESTAMP_WITH_TIMEZONE,
                        DECIMAL,
                        NUMERIC -> true;
                default -> false;
            };
        }

        // If you ever pass a vendor-specific SQLType (not JDBCType), typed binding is usually desirable.
        return true;
    }

    // ---- nullable readers ----

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
