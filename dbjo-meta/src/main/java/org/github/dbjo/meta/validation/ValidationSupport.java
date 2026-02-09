package org.github.dbjo.meta.validation;

import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.TableModel;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class ValidationSupport {
    private ValidationSupport() {
    }

    public static void throwIfAny(List<String> errors) {
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join("; ", errors));
        }
    }

    public static boolean isValidYyyyMmDd(Number value) {
        if (value == null) {
            return false;
        }
        long raw = value.longValue();
        if (raw < 10_000_000L || raw > 99_99_12_31L) {
            return false;
        }

        int ymd = (int) raw;
        int day = ymd % 100;
        int month = (ymd / 100) % 100;
        int year = ymd / 10_000;

        try {
            LocalDate.of(year, month, day);
            return true;
        } catch (DateTimeException ex) {
            return false;
        }
    }

    public static Map<String, Col> colsByName(TableModel tableModel) {
        Map<String, Col> out = new LinkedHashMap<>();
        if (tableModel == null || tableModel.cols() == null) {
            return out;
        }
        for (Col col : tableModel.cols()) {
            if (col != null && col.colName() != null) {
                out.put(col.colName().toUpperCase(Locale.ROOT), col);
            }
        }
        return out;
    }

    public static void validateNullableAndLength(List<String> errors, String columnName, Col col, Object value) {
        if (errors == null || columnName == null) {
            return;
        }
        if (col == null) {
            return;
        }

        if (!col.nullable() && value == null) {
            errors.add(columnName + " must not be null");
            return;
        }
        if (value == null) {
            return;
        }

        if (isStringLikeSqlType(col.sqlType()) && col.size() > 0 && value instanceof CharSequence cs && cs.length() > col.size()) {
            errors.add(columnName + " length must be <= " + col.size());
        }
        if (col.size() > 0 && value instanceof byte[] bytes && bytes.length > col.size()) {
            errors.add(columnName + " byte length must be <= " + col.size());
        }
    }

    private static boolean isStringLikeSqlType(int sqlType) {
        return sqlType == Types.CHAR
                || sqlType == Types.VARCHAR
                || sqlType == Types.NCHAR
                || sqlType == Types.NVARCHAR
                || sqlType == Types.LONGVARCHAR
                || sqlType == Types.LONGNVARCHAR;
    }
}
