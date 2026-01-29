package org.github.dbjo.rdb.jdbc.rowset;

import javax.sql.RowSetMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Minimal RowSetMetaData implementation to avoid com.sun.rowset dependency issues.
 * CachedRowSet only needs an implementation of RowSetMetaData with setters.
 */
public final class SimpleRowSetMetaData implements RowSetMetaData {
    private final int columnCount;

    private final int[] types;
    private final String[] typeNames;
    private final String[] names;
    private final int[] sizes;
    private final int[] scales;
    private final int[] nullables;

    public SimpleRowSetMetaData(int columnCount) {
        if (columnCount < 0) throw new IllegalArgumentException("columnCount");
        this.columnCount = columnCount;
        this.types = new int[columnCount];
        this.typeNames = new String[columnCount];
        this.names = new String[columnCount];
        this.sizes = new int[columnCount];
        this.scales = new int[columnCount];
        this.nullables = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            types[i] = java.sql.Types.VARCHAR;
            typeNames[i] = "VARCHAR";
            names[i] = "C" + (i + 1);
            sizes[i] = 0;
            scales[i] = 0;
            nullables[i] = ResultSetMetaData.columnNullableUnknown;
        }
    }

    private void check(int col) throws SQLException {
        if (col < 1 || col > columnCount) throw new SQLException("Bad column: " + col);
    }

    @Override public int getColumnCount() { return columnCount; }

    @Override public void setColumnType(int column, int sqlType) throws SQLException { check(column); types[column-1] = sqlType; }
    @Override public void setColumnTypeName(int column, String typeName) throws SQLException { check(column); typeNames[column-1] = typeName; }
    @Override public void setColumnLabel(int column, String label) throws SQLException { check(column); names[column-1] = label; }
    @Override public void setColumnName(int column, String columnName) throws SQLException { check(column); names[column-1] = columnName; }
    @Override public void setColumnDisplaySize(int column, int size) throws SQLException { check(column); sizes[column-1] = size; }
    @Override public void setScale(int column, int scale) throws SQLException { check(column); scales[column-1] = scale; }
    @Override public void setNullable(int column, int nullable) throws SQLException { check(column); nullables[column-1] = nullable; }

    // --- getters used by consumers ---

    @Override public String getColumnLabel(int column) throws SQLException { check(column); return names[column-1]; }
    @Override public String getColumnName(int column) throws SQLException { check(column); return names[column-1]; }
    @Override public int getColumnType(int column) throws SQLException { check(column); return types[column-1]; }
    @Override public String getColumnTypeName(int column) throws SQLException { check(column); return typeNames[column-1]; }
    @Override public int getColumnDisplaySize(int column) throws SQLException { check(column); return sizes[column-1]; }
    @Override public int getScale(int column) throws SQLException { check(column); return scales[column-1]; }
    @Override public int isNullable(int column) throws SQLException { check(column); return nullables[column-1]; }

    // --- ResultSetMetaData defaults (safe minimal) ---
    @Override public boolean isAutoIncrement(int column) { return false; }
    @Override public boolean isCaseSensitive(int column) { return true; }
    @Override public boolean isSearchable(int column) { return true; }
    @Override public boolean isCurrency(int column) { return false; }
    @Override public boolean isSigned(int column) { return true; }
    @Override public int getPrecision(int column) throws SQLException { check(column); return sizes[column-1]; }
    @Override public String getSchemaName(int column) { return ""; }
    @Override public String getTableName(int column) { return ""; }
    @Override public String getCatalogName(int column) { return ""; }
    @Override public boolean isReadOnly(int column) { return true; }
    @Override public boolean isWritable(int column) { return false; }
    @Override public boolean isDefinitelyWritable(int column) { return false; }
    @Override public String getColumnClassName(int column) { return "java.lang.Object"; }


    // unused setters required by interface:
    @Override public void setAutoIncrement(int column, boolean property) {}
    @Override public void setCaseSensitive(int column, boolean property) {}
    @Override public void setSearchable(int column, boolean property) {}
    @Override public void setCurrency(int column, boolean property) {}
    @Override public void setSigned(int column, boolean property) {}
    @Override public void setPrecision(int column, int precision) {}
    @Override public void setSchemaName(int column, String schemaName) {}
    @Override public void setTableName(int column, String tableName) {}
    @Override public void setCatalogName(int column, String catalogName) {}
    @Override public void setColumnCount(int columnCount) throws SQLException {}

    public void setReadOnly(int column, boolean property) {}
    public void setWritable(int column, boolean property) {}
    public void setDefinitelyWritable(int column, boolean property) {}
    public void setColumnClassName(int column, String className) {}

    // wrappers:
    @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("unwrap"); }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }
}
