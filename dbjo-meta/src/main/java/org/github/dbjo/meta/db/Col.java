package org.github.dbjo.meta.db;

public record Col(
        int pos,
        String colName,
        int sqlType,              // keep numeric
        String typeName,
        int size,
        int scale,
        Nullability nullability,  // enum instead of int
        boolean autoIncrement,    // boolean instead of "YES"/"NO"
        String defaultValue
) {
    public Col {
        if (nullability == null) nullability = Nullability.UNKNOWN;
    }

    public boolean nullable() {
        return nullability.isNullable();
    }
}
