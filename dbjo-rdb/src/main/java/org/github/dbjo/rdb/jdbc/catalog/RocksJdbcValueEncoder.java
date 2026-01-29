package org.github.dbjo.rdb.jdbc.catalog;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

public final class RocksJdbcValueEncoder {
    private RocksJdbcValueEncoder() {}

    public static byte[] encodeForColumn(RocksJdbcColumn column, Object value) throws SQLException {
        if (column == null || value == null) return null;

        return switch (column.sqlType()) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> encodeInt32(coerceInt(value));
            case Types.BIGINT -> encodeInt64(coerceLong(value));
            case Types.BIT, Types.BOOLEAN -> encodeInt32(coerceBooleanAsInt(value));
            case Types.CHAR, Types.NCHAR, Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR ->
                    String.valueOf(value).getBytes(StandardCharsets.UTF_8);
            default -> null;
        };
    }

    public static int coerceInt(Object value) throws SQLException {
        if (value instanceof BigDecimal bd) {
            try {
                return bd.intValueExact();
            } catch (ArithmeticException ex) {
                throw new SQLException("Numeric literal out of int range: " + value, ex);
            }
        }
        if (value instanceof Number n) {
            long l = n.longValue();
            if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                throw new SQLException("Numeric literal out of int range: " + value);
            }
            return (int) l;
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (NumberFormatException ex) {
            throw new SQLException("Expected integer literal, got: " + value, ex);
        }
    }

    public static long coerceLong(Object value) throws SQLException {
        if (value instanceof BigDecimal bd) {
            try {
                return bd.longValueExact();
            } catch (ArithmeticException ex) {
                throw new SQLException("Numeric literal out of long range: " + value, ex);
            }
        }
        if (value instanceof Number n) return n.longValue();
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException ex) {
            throw new SQLException("Expected long literal, got: " + value, ex);
        }
    }

    public static int coerceBooleanAsInt(Object value) throws SQLException {
        if (value instanceof Boolean b) return b ? 1 : 0;
        String s = String.valueOf(value).trim().toLowerCase(Locale.ROOT);
        if ("true".equals(s) || "1".equals(s)) return 1;
        if ("false".equals(s) || "0".equals(s)) return 0;
        throw new SQLException("Expected boolean literal, got: " + value);
    }

    public static byte[] encodeInt32(int value) {
        int x = value ^ 0x8000_0000;
        return new byte[] {
                (byte) (x >>> 24),
                (byte) (x >>> 16),
                (byte) (x >>> 8),
                (byte) x
        };
    }

    public static byte[] encodeInt64(long value) {
        long x = value ^ 0x8000_0000_0000_0000L;
        return new byte[] {
                (byte) (x >>> 56),
                (byte) (x >>> 48),
                (byte) (x >>> 40),
                (byte) (x >>> 32),
                (byte) (x >>> 24),
                (byte) (x >>> 16),
                (byte) (x >>> 8),
                (byte) x
        };
    }
}
