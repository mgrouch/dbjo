package org.github.dbjo.meta.jdbc;

import java.sql.*;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.UUID;

public final class Jdbc {
    private Jdbc() {}

    public static void bind(PreparedStatement ps, Object[] params, SQLType[] types) throws SQLException {
        if (params == null || params.length == 0) return;

        for (int i = 0; i < params.length; i++) {
            Object v = params[i];
            SQLType t = (types != null && i < types.length) ? types[i] : null;
            Integer vendor = (t == null) ? null : t.getVendorTypeNumber();

            if (v == null) {
                if (vendor != null) ps.setNull(i + 1, vendor);
                else ps.setNull(i + 1, Types.NULL);
                continue;
            }

            if (v instanceof byte[] b) {
                ps.setBytes(i + 1, b);
                continue;
            }

            if (shouldUseTypedSetObject(v, t)) {
                try {
                    ps.setObject(i + 1, v, t);
                    continue;
                } catch (SQLFeatureNotSupportedException ignored) {
                } catch (SQLException ignored) {
                }
            }

            if (vendor != null) {
                try {
                    ps.setObject(i + 1, v, vendor);
                    continue;
                } catch (SQLException ignored) {
                }
            }

            ps.setObject(i + 1, v);
        }
    }

    public static void addBatch(PreparedStatement ps, Object[] params, SQLType[] types) throws SQLException {
        bind(ps, params, types);
        ps.addBatch();
    }

    public static int sumBatchCounts(int[] counts) {
        if (counts == null) return 0;
        int sum = 0;
        for (int c : counts) {
            if (c == Statement.SUCCESS_NO_INFO) sum += 1;
            else if (c > 0) sum += c;
        }
        return sum;
    }

    public record BatchCountInfo(int sum, int successNoInfoCount, int failedCount) {}

    public static BatchCountInfo analyzeBatchCounts(int[] counts) {
        if (counts == null) return new BatchCountInfo(0, 0, 0);
        int sum = 0;
        int noInfo = 0;
        int failed = 0;
        for (int c : counts) {
            if (c == Statement.SUCCESS_NO_INFO) { sum += 1; noInfo++; }
            else if (c == Statement.EXECUTE_FAILED) { failed++; }
            else if (c > 0) sum += c;
        }
        return new BatchCountInfo(sum, noInfo, failed);
    }

    private static boolean shouldUseTypedSetObject(Object v, SQLType t) {
        if (t == null) return false;

        if (v instanceof OffsetDateTime) return true;
        if (v instanceof OffsetTime) return true;
        if (v instanceof UUID) return true;

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

        return true;
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
