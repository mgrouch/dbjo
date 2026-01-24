package org.github.dbjo.meta.db;

import java.sql.DatabaseMetaData;

/**
 * JDBC nullability tri-state from DatabaseMetaData.getColumns(...): NULLABLE column.
 */
public enum Nullability {
    NO_NULLS(DatabaseMetaData.columnNoNulls),
    NULLABLE(DatabaseMetaData.columnNullable),
    UNKNOWN(DatabaseMetaData.columnNullableUnknown);

    private final int jdbcCode;

    Nullability(int jdbcCode) {
        this.jdbcCode = jdbcCode;
    }

    public int jdbcCode() {
        return jdbcCode;
    }

    public boolean isNullable() {
        return this == NULLABLE;
    }

    public static Nullability fromJdbcCode(int code) {
        return switch (code) {
            case DatabaseMetaData.columnNoNulls -> NO_NULLS;
            case DatabaseMetaData.columnNullable -> NULLABLE;
            case DatabaseMetaData.columnNullableUnknown -> UNKNOWN;
            default -> UNKNOWN;
        };
    }
}
