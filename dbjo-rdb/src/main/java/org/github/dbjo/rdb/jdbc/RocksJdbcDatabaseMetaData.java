package org.github.dbjo.rdb.jdbc;

import org.github.dbjo.rdb.jdbc.catalog.*;
import org.github.dbjo.rdb.jdbc.rowset.SimpleRowSetMetaData;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import java.sql.*;
import java.util.*;

public final class RocksJdbcDatabaseMetaData implements DatabaseMetaData {
    private final RocksJdbcConnection conn;

    public RocksJdbcDatabaseMetaData(RocksJdbcConnection conn) {
        this.conn = conn;
    }

    private RocksJdbcCatalog catalog() { return conn.engine().catalog(); }

    // --- core identity ---
    @Override public String getDatabaseProductName() { return "dbjo-rocks"; }
    @Override public String getDatabaseProductVersion() { return "1.0"; }
    @Override public String getDriverName() { return "dbjo-rocks-jdbc"; }
    @Override public String getDriverVersion() { return "1.0"; }
    @Override public int getDriverMajorVersion() { return 1; }
    @Override public int getDriverMinorVersion() { return 0; }
    @Override public int getJDBCMajorVersion() { return 4; }
    @Override public int getJDBCMinorVersion() { return 2; }

    @Override public boolean allProceduresAreCallable() { return false; }
    @Override public boolean allTablesAreSelectable() { return true; }
    @Override public boolean isReadOnly() { return true; }

    // --- schemas ---
    @Override
    public ResultSet getSchemas() throws SQLException {
        CachedRowSet rs = RowSetProvider.newFactory().createCachedRowSet();
        SimpleRowSetMetaData md = new SimpleRowSetMetaData(2);
        md.setColumnName(1, "TABLE_SCHEM");
        md.setColumnType(1, Types.VARCHAR);
        md.setColumnName(2, "TABLE_CATALOG");
        md.setColumnType(2, Types.VARCHAR);
        rs.setMetaData(md);

        rs.moveToInsertRow();
        rs.updateString(1, "PUBLIC");
        rs.updateString(2, null);
        rs.insertRow();
        rs.moveToCurrentRow();

        rs.beforeFirst();
        return rs;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        CachedRowSet rs = RowSetProvider.newFactory().createCachedRowSet();
        // Standard columns DataGrip expects:
        // TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS
        SimpleRowSetMetaData md = new SimpleRowSetMetaData(5);
        md.setColumnName(1, "TABLE_CAT");   md.setColumnType(1, Types.VARCHAR);
        md.setColumnName(2, "TABLE_SCHEM"); md.setColumnType(2, Types.VARCHAR);
        md.setColumnName(3, "TABLE_NAME");  md.setColumnType(3, Types.VARCHAR);
        md.setColumnName(4, "TABLE_TYPE");  md.setColumnType(4, Types.VARCHAR);
        md.setColumnName(5, "REMARKS");     md.setColumnType(5, Types.VARCHAR);
        rs.setMetaData(md);

        for (RocksJdbcTable t : catalog().tables()) {
            if (!match(schemaPattern, t.schemaName())) continue;
            if (!match(tableNamePattern, t.tableName())) continue;

            rs.moveToInsertRow();
            rs.updateString(1, null);
            rs.updateString(2, t.schemaName());
            rs.updateString(3, t.tableName());
            rs.updateString(4, "TABLE");
            rs.updateString(5, "");
            rs.insertRow();
            rs.moveToCurrentRow();
        }

        rs.beforeFirst();
        return rs;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        CachedRowSet rs = RowSetProvider.newFactory().createCachedRowSet();

        // A subset of standard getColumns columns that tools use heavily:
        // TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_SIZE, DECIMAL_DIGITS, NULLABLE, ORDINAL_POSITION
        SimpleRowSetMetaData md = new SimpleRowSetMetaData(9);
        md.setColumnName(1, "TABLE_SCHEM"); md.setColumnType(1, Types.VARCHAR);
        md.setColumnName(2, "TABLE_NAME");  md.setColumnType(2, Types.VARCHAR);
        md.setColumnName(3, "COLUMN_NAME"); md.setColumnType(3, Types.VARCHAR);
        md.setColumnName(4, "DATA_TYPE");   md.setColumnType(4, Types.INTEGER);
        md.setColumnName(5, "TYPE_NAME");   md.setColumnType(5, Types.VARCHAR);
        md.setColumnName(6, "COLUMN_SIZE"); md.setColumnType(6, Types.INTEGER);
        md.setColumnName(7, "DECIMAL_DIGITS"); md.setColumnType(7, Types.INTEGER);
        md.setColumnName(8, "NULLABLE");    md.setColumnType(8, Types.INTEGER);
        md.setColumnName(9, "ORDINAL_POSITION"); md.setColumnType(9, Types.INTEGER);
        rs.setMetaData(md);

        for (RocksJdbcTable t : catalog().tables()) {
            if (!match(schemaPattern, t.schemaName())) continue;
            if (!match(tableNamePattern, t.tableName())) continue;

            for (RocksJdbcColumn c : t.columns()) {
                if (!match(columnNamePattern, c.name())) continue;

                rs.moveToInsertRow();
                rs.updateString(1, t.schemaName());
                rs.updateString(2, t.tableName());
                rs.updateString(3, c.name());
                rs.updateInt(4, c.sqlType());
                rs.updateString(5, c.typeName());
                rs.updateInt(6, c.size());
                rs.updateInt(7, c.scale());
                rs.updateInt(8, c.nullable());
                rs.updateInt(9, c.pos());
                rs.insertRow();
                rs.moveToCurrentRow();
            }
        }

        rs.beforeFirst();
        return rs;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        RocksJdbcTable t = catalog().requireTable(table);

        CachedRowSet rs = RowSetProvider.newFactory().createCachedRowSet();
        // TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, KEY_SEQ, PK_NAME
        SimpleRowSetMetaData md = new SimpleRowSetMetaData(5);
        md.setColumnName(1, "TABLE_SCHEM"); md.setColumnType(1, Types.VARCHAR);
        md.setColumnName(2, "TABLE_NAME");  md.setColumnType(2, Types.VARCHAR);
        md.setColumnName(3, "COLUMN_NAME"); md.setColumnType(3, Types.VARCHAR);
        md.setColumnName(4, "KEY_SEQ");     md.setColumnType(4, Types.SMALLINT);
        md.setColumnName(5, "PK_NAME");     md.setColumnType(5, Types.VARCHAR);
        rs.setMetaData(md);

        String[] pk = t.pkColumns();
        for (int i = 0; i < pk.length; i++) {
            rs.moveToInsertRow();
            rs.updateString(1, t.schemaName());
            rs.updateString(2, t.tableName());
            rs.updateString(3, pk[i]);
            rs.updateInt(4, i + 1);
            rs.updateString(5, "PRIMARY");
            rs.insertRow();
            rs.moveToCurrentRow();
        }

        rs.beforeFirst();
        return rs;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        RocksJdbcTable t = catalog().requireTable(table);

        CachedRowSet rs = RowSetProvider.newFactory().createCachedRowSet();
        // TABLE_SCHEM, TABLE_NAME, NON_UNIQUE, INDEX_NAME, ORDINAL_POSITION, COLUMN_NAME
        SimpleRowSetMetaData md = new SimpleRowSetMetaData(6);
        md.setColumnName(1, "TABLE_SCHEM"); md.setColumnType(1, Types.VARCHAR);
        md.setColumnName(2, "TABLE_NAME"); md.setColumnType(2, Types.VARCHAR);
        md.setColumnName(3, "NON_UNIQUE"); md.setColumnType(3, Types.BOOLEAN);
        md.setColumnName(4, "INDEX_NAME"); md.setColumnType(4, Types.VARCHAR);
        md.setColumnName(5, "ORDINAL_POSITION"); md.setColumnType(5, Types.SMALLINT);
        md.setColumnName(6, "COLUMN_NAME"); md.setColumnType(6, Types.VARCHAR);
        rs.setMetaData(md);

        // primary as index
        String[] pk = t.pkColumns();
        if (pk.length > 0 && (!unique || true)) {
            for (int i = 0; i < pk.length; i++) {
                rs.moveToInsertRow();
                rs.updateString(1, t.schemaName());
                rs.updateString(2, t.tableName());
                rs.updateBoolean(3, false);
                rs.updateString(4, "PRIMARY");
                rs.updateInt(5, i + 1);
                rs.updateString(6, pk[i]);
                rs.insertRow();
                rs.moveToCurrentRow();
            }
        }

        for (RocksJdbcIndex ix : t.indexes()) {
            if (ix == null) continue;
            if (unique && !ix.unique()) continue;
            String[] cols = ix.columnNames();
            for (int i = 0; i < cols.length; i++) {
                rs.moveToInsertRow();
                rs.updateString(1, t.schemaName());
                rs.updateString(2, t.tableName());
                rs.updateBoolean(3, !ix.unique());
                rs.updateString(4, ix.indexName());
                rs.updateInt(5, i + 1);
                rs.updateString(6, cols[i]);
                rs.insertRow();
                rs.moveToCurrentRow();
            }
        }

        rs.beforeFirst();
        return rs;
    }

    // --- helpers ---
    private static boolean match(String pattern, String value) {
        if (pattern == null || pattern.isBlank()) return true;
        if (value == null) return false;

        String p = pattern.replace("%", ".*").replace("_", ".");
        return value.matches("(?i)" + p);
    }

    // --- Required by interface but not used by most tools ---
    @Override public Connection getConnection() { return conn; }

    // --- lots of unsupported methods ---
    private static SQLFeatureNotSupportedException unsup() { return new SQLFeatureNotSupportedException(); }

    @Override public ResultSet getCatalogs() throws SQLException { throw unsup(); }
    @Override public ResultSet getTableTypes() throws SQLException { throw unsup(); }
    @Override public ResultSet getTypeInfo() throws SQLException { throw unsup(); }

    // Capability flags (safe conservative)
    @Override public boolean supportsTransactions() { return false; }
    @Override public boolean supportsResultSetType(int type) { return type == ResultSet.TYPE_FORWARD_ONLY; }
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) { return concurrency == ResultSet.CONCUR_READ_ONLY; }

    // Everything else: default unsupported or conservative false
    @Override public boolean supportsStoredProcedures() { return false; }
    @Override public boolean supportsBatchUpdates() { return false; }
    @Override public boolean supportsSavepoints() { return false; }
    @Override public boolean supportsNamedParameters() { return false; }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw unsup(); }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }

    // --- auto-generated stubs (keep minimal) ---
    @Override public boolean nullsAreSortedHigh() { return false; }
    @Override public boolean nullsAreSortedLow() { return true; }
    @Override public boolean nullsAreSortedAtStart() { return false; }
    @Override public boolean nullsAreSortedAtEnd() { return true; }
    @Override public String getURL() { return null; }
    @Override public String getUserName() { return ""; }
    @Override public boolean usesLocalFiles() { return true; }
    @Override public boolean usesLocalFilePerTable() { return false; }
    @Override public boolean supportsMixedCaseIdentifiers() { return true; }
    @Override public boolean storesUpperCaseIdentifiers() { return false; }
    @Override public boolean storesLowerCaseIdentifiers() { return false; }
    @Override public boolean storesMixedCaseIdentifiers() { return true; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean storesUpperCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesMixedCaseQuotedIdentifiers() { return true; }
    @Override public String getIdentifierQuoteString() { return "\""; }
    @Override public String getSQLKeywords() { return ""; }
    @Override public String getNumericFunctions() { return ""; }
    @Override public String getStringFunctions() { return ""; }
    @Override public String getSystemFunctions() { return ""; }
    @Override public String getTimeDateFunctions() { return ""; }
    @Override public String getSearchStringEscape() { return "\\"; }
    @Override public String getExtraNameCharacters() { return "_"; }
    @Override public boolean supportsAlterTableWithAddColumn() { return false; }
    @Override public boolean supportsAlterTableWithDropColumn() { return false; }
    @Override public boolean supportsColumnAliasing() { return true; }
    @Override public boolean supportsTableCorrelationNames() { return true; }
    @Override public boolean supportsDifferentTableCorrelationNames() { return false; }
    @Override public boolean supportsExpressionsInOrderBy() { return false; }
    @Override public boolean supportsOrderByUnrelated() { return false; }
    @Override public boolean supportsGroupBy() { return false; }
    @Override public boolean supportsGroupByUnrelated() { return false; }
    @Override public boolean supportsGroupByBeyondSelect() { return false; }
    @Override public boolean supportsLikeEscapeClause() { return false; }
    @Override public boolean supportsMultipleResultSets() { return false; }
    @Override public boolean supportsMultipleTransactions() { return false; }
    @Override public boolean supportsNonNullableColumns() { return true; }
    @Override public boolean supportsMinimumSQLGrammar() { return true; }
    @Override public boolean supportsCoreSQLGrammar() { return false; }
    @Override public boolean supportsExtendedSQLGrammar() { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() { return false; }
    @Override public boolean supportsANSI92IntermediateSQL() { return false; }
    @Override public boolean supportsANSI92FullSQL() { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() { return false; }
    @Override public boolean supportsOuterJoins() { return false; }
    @Override public boolean supportsFullOuterJoins() { return false; }
    @Override public boolean supportsLimitedOuterJoins() { return false; }

    // Remaining DatabaseMetaData methods omitted: if your compiler requires them, generate stubs returning false/unsup.
}
