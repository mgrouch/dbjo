package org.github.dbjo.codegen.db;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.model.Col;
import org.github.dbjo.codegen.model.IndexModel;
import org.github.dbjo.codegen.model.TableModel;
import org.github.dbjo.codegen.model.TableRef;

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
                .comparing((TableModel tm) -> tm.table().schema().toUpperCase(Locale.ROOT))
                .thenComparing(tm -> tm.table().table().toUpperCase(Locale.ROOT)));

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
        List<TableRef> out = new ArrayList<>();
        try (ResultSet rs = meta.getTables(null, null, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                String table  = rs.getString("TABLE_NAME");
                if (schema == null || table == null) continue;
                if (isSystemSchema(schema)) continue;
                out.add(new TableRef(schema, table));
            }
        }
        out.sort(Comparator
                .comparing((TableRef t) -> t.schema().toUpperCase(Locale.ROOT))
                .thenComparing(t -> t.table().toUpperCase(Locale.ROOT)));
        return out;
    }

    private static List<Col> listColumns(DatabaseMetaData meta, String schema, String table) throws SQLException {
        List<Col> cols = new ArrayList<>();
        try (ResultSet crs = meta.getColumns(null, schema, table, "%")) {
            while (crs.next()) {
                cols.add(new Col(
                        crs.getInt("ORDINAL_POSITION"),
                        crs.getString("COLUMN_NAME"),
                        crs.getInt("DATA_TYPE"),
                        crs.getString("TYPE_NAME"),
                        crs.getInt("COLUMN_SIZE"),
                        crs.getInt("DECIMAL_DIGITS"),
                        crs.getInt("NULLABLE"),
                        safeGet(crs, "IS_AUTOINCREMENT"),
                        safeGet(crs, "COLUMN_DEF")
                ));
            }
        }
        cols.sort(Comparator.comparingInt(Col::pos));
        return cols;
    }

    private static Set<String> getPrimaryKeyColumns(DatabaseMetaData meta, String schema, String table) throws SQLException {
        Set<String> pk = new HashSet<>();
        try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME");
                if (col != null) pk.add(col.toUpperCase(Locale.ROOT));
            }
        }
        return pk;
    }

    private static List<IndexModel> listIndexes(DatabaseMetaData meta, String schema, String table) throws SQLException {
        // indexName -> (unique?, ordinal->colName)
        class Agg {
            boolean unique = false;
            final Map<Integer, String> colsByOrd = new TreeMap<>();
        }

        Map<String, Agg> map = new LinkedHashMap<>();

        try (ResultSet rs = meta.getIndexInfo(null, schema, table, false, false)) {
            while (rs.next()) {
                short type = rs.getShort("TYPE");
                if (type == DatabaseMetaData.tableIndexStatistic) continue;

                String idxName = rs.getString("INDEX_NAME");
                String colName = rs.getString("COLUMN_NAME");
                int ord = rs.getInt("ORDINAL_POSITION");

                if (idxName == null || idxName.isBlank()) continue;
                if (colName == null || colName.isBlank()) continue;
                if (ord <= 0) ord = 1;

                boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                boolean unique = !nonUnique;

                Agg agg = map.computeIfAbsent(idxName, k -> new Agg());
                agg.unique = agg.unique || unique; // if any row says unique, treat as unique
                agg.colsByOrd.put(ord, colName);
            }
        }

        List<IndexModel> out = new ArrayList<>(map.size());
        for (var e : map.entrySet()) {
            List<String> cols = new ArrayList<>(e.getValue().colsByOrd.values());
            out.add(new IndexModel(e.getKey(), e.getValue().unique, cols));
        }

        out.sort(Comparator.comparing(IndexModel::indexName, String.CASE_INSENSITIVE_ORDER));
        return out;
    }

    private static boolean isSystemSchema(String schema) {
        String s = schema.toUpperCase(Locale.ROOT);
        return s.equals("INFORMATION_SCHEMA") || s.startsWith("SYSTEM") || s.startsWith("SYS");
    }

    private static String safeGet(ResultSet rs, String col) {
        try { return rs.getString(col); }
        catch (SQLException ignored) { return null; }
    }
}
