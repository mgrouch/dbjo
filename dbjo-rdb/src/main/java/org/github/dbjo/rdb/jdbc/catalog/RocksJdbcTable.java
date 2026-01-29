package org.github.dbjo.rdb.jdbc.catalog;

import org.github.dbjo.criteria.PropertyTerm;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class RocksJdbcTable {
    private final String schemaName; // e.g. "PUBLIC"
    private final String tableName;
    private final String cfName;

    private final Class<?> rowClass;

    private final RocksJdbcColumn[] columns;
    private final String[] columnNames;
    private final int[] columnSqlTypes;
    private final String[] getterNames;

    private final String[] pkColumns;     // column names in key order (best-effort)
    private final RocksJdbcIndex[] indexes;

    private final RocksJdbcDecoder decoder;
    private final Map<String, PropertyTerm<?, ? extends Serializable>> termsByColumnLower;

    private final String[] names; // aliases for lookup (table, cf, class-name, etc.)

    public RocksJdbcTable(
            String schemaName,
            String tableName,
            String cfName,
            Class<?> rowClass,
            RocksJdbcColumn[] columns,
            String[] pkColumns,
            RocksJdbcIndex[] indexes,
            RocksJdbcDecoder decoder,
            Map<String, PropertyTerm<?, ? extends Serializable>> termsByColumnLower,
            String[] names
    ) {
        this.schemaName = (schemaName == null || schemaName.isBlank()) ? "PUBLIC" : schemaName;
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.cfName = Objects.requireNonNull(cfName, "cfName");
        this.rowClass = Objects.requireNonNull(rowClass, "rowClass");
        this.columns = Objects.requireNonNull(columns, "columns").clone();
        this.pkColumns = (pkColumns == null) ? new String[0] : pkColumns.clone();
        this.indexes = (indexes == null) ? new RocksJdbcIndex[0] : indexes.clone();
        this.decoder = Objects.requireNonNull(decoder, "decoder");
        this.termsByColumnLower = (termsByColumnLower == null) ? Map.of() : new HashMap<>(termsByColumnLower);
        this.names = (names == null || names.length == 0) ? new String[]{ tableName } : names.clone();

        this.columnNames = new String[this.columns.length];
        this.columnSqlTypes = new int[this.columns.length];
        this.getterNames = new String[this.columns.length];

        for (int i = 0; i < this.columns.length; i++) {
            RocksJdbcColumn c = this.columns[i];
            this.columnNames[i] = c.name();
            this.columnSqlTypes[i] = c.sqlType();
            this.getterNames[i] = c.getterName();
        }
    }

    public String schemaName() { return schemaName; }
    public String tableName() { return tableName; }
    public String cfName() { return cfName; }
    public Class<?> rowClass() { return rowClass; }

    public RocksJdbcColumn[] columns() { return columns.clone(); }
    public String[] columnNames() { return columnNames.clone(); }
    public int[] columnSqlTypes() { return columnSqlTypes.clone(); }
    public String[] getterNames() { return getterNames.clone(); }

    public String[] pkColumns() { return pkColumns.clone(); }
    public RocksJdbcIndex[] indexes() { return indexes.clone(); }

    public RocksJdbcDecoder decoder() { return decoder; }
    public Map<String, PropertyTerm<?, ? extends Serializable>> termsByColumnLower() { return Map.copyOf(termsByColumnLower); }

    public List<String> names() { return List.of(names); }

    public boolean nameMatches(String patternOrName) {
        if (patternOrName == null) return false;
        String k = patternOrName.trim().toLowerCase(Locale.ROOT);
        if (k.isEmpty()) return false;
        for (String n : names) {
            if (n != null && n.trim().equalsIgnoreCase(k)) return true;
        }
        return tableName.equalsIgnoreCase(k);
    }

    @Override
    public String toString() {
        return "RocksJdbcTable{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", cfName='" + cfName + '\'' +
                ", rowClass=" + rowClass.getName() +
                ", columns=" + Arrays.toString(columnNames) +
                '}';
    }
}
