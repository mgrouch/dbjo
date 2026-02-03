package org.github.dbjo.codegen.db;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.IndexModel;
import org.github.dbjo.meta.db.Nullability;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.meta.db.TableRef;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public final class DbIntrospector {
    private final Config cfg;

    public DbIntrospector(Config cfg) {
        this.cfg = cfg;
    }

    public List<TableModel> loadTables(DatabaseMetaData meta) throws SQLException {
        Objects.requireNonNull(meta, "meta");

        List<TableRef> tables = listUserTables(meta);
        tables = applyFilters(tables);

        List<TableModel> out = new ArrayList<>(tables.size());
        for (TableRef t : tables) {
            List<Col> cols = listColumns(meta, t.schema(), t.table());
            if (cols.isEmpty()) continue;

            Set<String> pk = getPrimaryKeyColumns(meta, t.schema(), t.table());
            List<IndexModel> idx = listIndexes(meta, t.schema(), t.table());

            out.add(new TableModel(t, cols, pk, idx));
        }

        out.sort(Comparator
                .comparing((TableModel tm) -> nz(tm.table() == null ? null : tm.table().schema()).toUpperCase(Locale.ROOT))
                .thenComparing(tm -> nz(tm.table() == null ? null : tm.table().table()).toUpperCase(Locale.ROOT)));

        return out;
    }

    private List<TableRef> applyFilters(List<TableRef> in) {
        if (cfg.schemaInclude() == null && cfg.schemaExclude() == null &&
                cfg.tableInclude() == null && cfg.tableExclude() == null) {
            return in;
        }
        List<TableRef> out = new ArrayList<>(in.size());
        for (TableRef t : in) {
            String schema = t.schema() == null ? "" : t.schema();
            String table  = t.table() == null ? "" : t.table();

            if (cfg.schemaInclude() != null && !cfg.schemaInclude().matcher(schema).find()) continue;
            if (cfg.schemaExclude() != null &&  cfg.schemaExclude().matcher(schema).find()) continue;
            if (cfg.tableInclude()  != null && !cfg.tableInclude().matcher(table).find()) continue;
            if (cfg.tableExclude()  != null &&  cfg.tableExclude().matcher(table).find()) continue;

            out.add(t);
        }
        return out;
    }

    private static List<TableRef> listUserTables(DatabaseMetaData meta) throws SQLException {
        final String product = safeUpper(safeGet(meta::getDatabaseProductName));
        final String catalog = safeCatalog(meta);

        List<TableRef> out = new ArrayList<>();

        // Most drivers: (catalog, schemaPattern=null, tablePattern="%", types={"TABLE","VIEW"})
        try (ResultSet rs = meta.getTables(catalog, null, "%", new String[]{"TABLE", "VIEW"})) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                String table  = rs.getString("TABLE_NAME");

                // For MSSQL/Oracle/Sybase schema should exist; still be defensive.
                if (table == null || table.isBlank()) continue;
                if (schema == null || schema.isBlank()) continue;

                if (isSystemSchema(product, schema)) continue;

                out.add(new TableRef(schema, table));
            }
        }

        out.sort(Comparator
                .comparing((TableRef t) -> nz(t.schema()).toUpperCase(Locale.ROOT))
                .thenComparing(t -> nz(t.table()).toUpperCase(Locale.ROOT)));

        return out;
    }

    private static List<Col> listColumns(DatabaseMetaData meta, String schema, String table) throws SQLException {
        final String catalog = safeCatalog(meta);

        List<Col> cols = new ArrayList<>();
        try (ResultSet crs = meta.getColumns(catalog, schema, table, "%")) {
            while (crs.next()) {
                int sqlType = crs.getInt("DATA_TYPE"); // numeric java.sql.Types

                int nullableCode = crs.getInt("NULLABLE");
                Nullability nullability = Nullability.fromJdbcCode(nullableCode);

                // Some drivers: "YES"/"NO", some: null/"" (unknown)
                String ai = safeGet(crs, "IS_AUTOINCREMENT");
                boolean autoInc = "YES".equalsIgnoreCase(ai);

                cols.add(new Col(
                        crs.getInt("ORDINAL_POSITION"),
                        crs.getString("COLUMN_NAME"),
                        sqlType,
                        crs.getString("TYPE_NAME"),
                        crs.getInt("COLUMN_SIZE"),
                        crs.getInt("DECIMAL_DIGITS"),
                        nullability,
                        autoInc,
                        safeGet(crs, "COLUMN_DEF")
                ));
            }
        }

        cols.sort(Comparator.comparingInt(Col::pos));
        return cols;
    }

    private static Set<String> getPrimaryKeyColumns(DatabaseMetaData meta, String schema, String table) throws SQLException {
        final String catalog = safeCatalog(meta);

        Set<String> pk = new HashSet<>();
        try (ResultSet rs = meta.getPrimaryKeys(catalog, schema, table)) {
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME");
                if (col != null && !col.isBlank()) pk.add(col.toUpperCase(Locale.ROOT));
            }
        }
        return pk;
    }

    private static List<IndexModel> listIndexes(DatabaseMetaData meta, String schema, String table) {
        final String catalog = safeCatalog(meta);

        // indexName -> (unique?, ordinal->colName)
        class Agg {
            boolean unique = false;
            boolean uniqueKnown = false; // track if we ever parsed NON_UNIQUE
            int nextOrd = 1;             // fallback ordinal when driver returns 0/NULL
            final Map<Integer, String> colsByOrd = new TreeMap<>();
        }

        Map<String, Agg> map = new LinkedHashMap<>();

        // approximate=true improves compatibility for some drivers
        try (ResultSet rs = meta.getIndexInfo(catalog, schema, table, false, true)) {
            while (rs.next()) {
                short type = rs.getShort("TYPE");
                if (type == DatabaseMetaData.tableIndexStatistic) continue;

                String idxName = rs.getString("INDEX_NAME");
                String colName = rs.getString("COLUMN_NAME");
                int ord = rs.getInt("ORDINAL_POSITION");

                if (idxName == null || idxName.isBlank()) continue;
                if (colName == null || colName.isBlank()) continue;

                Agg agg = map.computeIfAbsent(idxName, k -> new Agg());

                // NON_UNIQUE can be weird/null-ish depending on driver; be defensive.
                Boolean nonUnique = safeGetBoolean(rs, "NON_UNIQUE");
                if (nonUnique != null) {
                    boolean unique = !nonUnique;
                    if (!agg.uniqueKnown) {
                        agg.unique = unique;
                        agg.uniqueKnown = true;
                    } else {
                        // If inconsistent rows appear, keep "unique if any row says unique"
                        agg.unique = agg.unique || unique;
                    }
                }

                // Ordinal robustness: some drivers give 0 for ordinals.
                if (ord <= 0) {
                    ord = agg.nextOrd++;
                } else {
                    agg.nextOrd = Math.max(agg.nextOrd, ord + 1);
                }

                // Avoid collision (drivers sometimes repeat ord)
                while (agg.colsByOrd.containsKey(ord)) ord = agg.nextOrd++;

                agg.colsByOrd.put(ord, colName);
            }
        } catch (SQLException e) {
            // Some drivers can throw for getIndexInfo; fail soft (indexes optional for generation).
            return List.of();
        }

        List<IndexModel> out = new ArrayList<>(map.size());
        for (var e : map.entrySet()) {
            List<String> cols = new ArrayList<>(e.getValue().colsByOrd.values());
            boolean unique = e.getValue().uniqueKnown && e.getValue().unique;
            out.add(new IndexModel(e.getKey(), unique, cols));
        }

        out.sort(Comparator.comparing(IndexModel::indexName, String.CASE_INSENSITIVE_ORDER));
        return out;
    }

    private static boolean isSystemSchema(String productUpper, String schema) {
        String s = schema == null ? "" : schema.trim();
        if (s.isEmpty()) return true;

        String su = s.toUpperCase(Locale.ROOT);

        // Common everywhere
        if ("INFORMATION_SCHEMA".equals(su)) return true;

        // SQL Server / Sybase (system schemas)
        if (productUpper.contains("MICROSOFT") || productUpper.contains("SQL SERVER") || productUpper.contains("SYBASE")) {
            return "SYS".equals(su) || su.startsWith("SYS");
        }

        // Oracle: lots of well-known system schemas
        if (productUpper.contains("ORACLE")) {
            if (ORACLE_SYSTEM_SCHEMAS.contains(su)) return true;
            if (su.startsWith("APEX_") || su.startsWith("FLOWS_")) return true;
            if (su.startsWith("XS$")) return true;
            // Generic SYS* still counts for Oracle
            return "SYS".equals(su) || su.startsWith("SYS");
        }

        // Generic fallback
        return "SYS".equals(su) || su.startsWith("SYS") || "SYSTEM".equals(su) || su.startsWith("SYSTEM");
    }

    private static final Set<String> ORACLE_SYSTEM_SCHEMAS = Set.of(
            "SYS", "SYSTEM", "SYSAUX", "XDB", "CTXSYS", "MDSYS", "WMSYS", "ORDSYS",
            "OUTLN", "DBSNMP", "AUDSYS", "OJVMSYS", "ORACLE_OCM", "APPQOSSYS",
            "DVSYS", "DVF", "LBACSYS", "GSMADMIN_INTERNAL", "ANONYMOUS", "EXFSYS",
            "OLAPSYS", "GGSYS", "REMOTE_SCHEDULER_AGENT", "SI_INFORMTN_SCHEMA"
    );

    private static String safeCatalog(DatabaseMetaData meta) {
        try {
            Connection c = meta.getConnection();
            if (c == null) return null;
            String cat = c.getCatalog();
            return (cat == null || cat.isBlank()) ? null : cat;
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String safeGet(ResultSet rs, String col) {
        try { return rs.getString(col); }
        catch (SQLException ignored) { return null; }
    }

    private static Boolean safeGetBoolean(ResultSet rs, String col) {
        try {
            Object o = rs.getObject(col);
            if (o == null) return null;
            if (o instanceof Boolean b) return b;
            if (o instanceof Number n) return n.intValue() != 0;
            String s = String.valueOf(o).trim();
            if (s.isEmpty()) return null;
            if ("1".equals(s)) return true;
            if ("0".equals(s)) return false;
            if ("TRUE".equalsIgnoreCase(s)) return true;
            if ("FALSE".equalsIgnoreCase(s)) return false;
            return null;
        } catch (SQLException ignored) {
            return null;
        }
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    private static String safeUpper(String s) {
        return s == null ? "" : s.toUpperCase(Locale.ROOT);
    }

    @FunctionalInterface
    private interface SqlSupplier<T> { T get() throws SQLException; }

    private static <T> T safeGet(SqlSupplier<T> fn) {
        try { return fn.get(); } catch (SQLException e) { return null; }
    }
}
