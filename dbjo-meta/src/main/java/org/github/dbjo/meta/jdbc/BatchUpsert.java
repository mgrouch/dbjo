package org.github.dbjo.meta.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Objects;

/**
 * Batch upsert helper.
 *
 * Strategy:
 *  - MSSQL/SYBASE: temp-table load (batched inserts) + single MERGE from temp.
 *  - ORACLE/HSQL: batch regular per-row upsert (MERGE ... USING dual / HSQL MERGE ... VALUES).
 */
public final class BatchUpsert {
    private BatchUpsert() {}

    /**
     * For temp-table strategy:
     *  - rowsInsertedIntoTemp: sum of insert-batch counts
     *  - mergeAffectedRows: affected rows from MERGE FROM TEMP
     *
     * For direct-batch strategy (ORACLE/HSQL):
     *  - rowsInsertedIntoTemp: 0
     *  - mergeAffectedRows: sum of upsert-batch counts
     */
    public record Result(int rowsInsertedIntoTemp, int mergeAffectedRows) {}

    public static <T> Builder<T> forMeta(DbMeta<T> meta) {
        return new Builder<>(meta);
    }

    public static final class Builder<T> {
        private final DbMeta<T> meta;

        private DbDialect dialect;
        private String suffix = "X";

        private int batchSize = 500;

        /** If true and dialect supports temp upsert, attempt CREATE temp table. */
        private boolean ensureTempTable = true;

        /** If true and dialect supports temp upsert, do DELETE FROM temp before loading. */
        private boolean clearTempBeforeLoad = false;

        /**
         * Drop temp table at end (only relevant for MSSQL/SYBASE temp strategy).
         * Default: true for MSSQL/SYBASE, false otherwise.
         */
        private Boolean dropTempTable = null;

        /** If true, ignore create-table errors that look like "already exists". */
        private boolean ignoreAlreadyExistsOnCreate = true;

        /** Optional statement timeout (0 = driver default). */
        private int statementTimeoutSeconds = 0;

        /** Allow opting out of temp strategy even if supported (force direct batching). */
        private boolean preferTempWhenAvailable = true;

        private Builder(DbMeta<T> meta) {
            this.meta = Objects.requireNonNull(meta, "meta");
        }

        public Builder<T> dialect(DbDialect d) { this.dialect = Objects.requireNonNull(d, "dialect"); return this; }
        public Builder<T> suffix(String s) { this.suffix = (s == null || s.isBlank()) ? "X" : s; return this; }

        public Builder<T> batchSize(int n) {
            if (n <= 0) throw new IllegalArgumentException("batchSize must be > 0");
            this.batchSize = n;
            return this;
        }

        public Builder<T> ensureTempTable(boolean v) { this.ensureTempTable = v; return this; }
        public Builder<T> clearTempBeforeLoad(boolean v) { this.clearTempBeforeLoad = v; return this; }

        public Builder<T> dropTempTable(boolean v) { this.dropTempTable = v; return this; }

        public Builder<T> ignoreAlreadyExistsOnCreate(boolean v) { this.ignoreAlreadyExistsOnCreate = v; return this; }

        public Builder<T> statementTimeoutSeconds(int seconds) {
            if (seconds < 0) throw new IllegalArgumentException("statementTimeoutSeconds must be >= 0");
            this.statementTimeoutSeconds = seconds;
            return this;
        }

        /** If false, always use direct batching even on MSSQL/SYBASE. */
        public Builder<T> preferTempWhenAvailable(boolean v) { this.preferTempWhenAvailable = v; return this; }

        public Result execute(Connection con, Iterable<T> rows) throws SQLException {
            Objects.requireNonNull(con, "con");
            Objects.requireNonNull(rows, "rows");
            if (dialect == null) throw new IllegalStateException("dialect not set");

            Iterator<T> it = rows.iterator();
            if (!it.hasNext()) return new Result(0, 0);

            // Oracle: NO temp-table strategy by design.
            // HSQL: NO temp-table strategy by design (unless you later decide otherwise).
            boolean canTemp = meta.supportsUpsertTemp(dialect);
            boolean useTemp = preferTempWhenAvailable && canTemp;

            if (!useTemp) {
                int affected = batchUpsertDirect(con, it);
                return new Result(0, affected);
            }

            boolean drop = (dropTempTable != null)
                    ? dropTempTable
                    : (dialect == DbDialect.MSSQL || dialect == DbDialect.SYBASE);

            int inserted = 0;
            int merged = 0;

            try {
                if (ensureTempTable) {
                    String ddl = meta.createUpsertTempTableSql(dialect, suffix);
                    execSql(con, ddl, true);
                }

                if (clearTempBeforeLoad) {
                    // Parse temp table name from INSERT SQL to avoid reconstructing naming rules here.
                    String insSql = meta.insertUpsertTempSql(dialect, suffix);
                    String tn = parseInsertIntoTableName(insSql);
                    execSql(con, "DELETE FROM " + tn, false);
                }

                inserted = batchInsertTemp(con, it);

                String mergeSql = meta.mergeUpsertFromTempSql(dialect, suffix);
                merged = execUpdate(con, mergeSql);

                if (drop) {
                    String dropSql = meta.dropUpsertTempTableSql(dialect, suffix);
                    execSql(con, dropSql, false);
                }

                return new Result(inserted, merged);
            } catch (SQLException e) {
                throw e;
            }
        }

        private int batchUpsertDirect(Connection con, Iterator<T> it) throws SQLException {
            final String sql = meta.upsertByIdSql(dialect);
            final SQLType[] types = meta.upsertByIdParamTypes();

            int affected = 0;
            int pending = 0;

            try (PreparedStatement ps = con.prepareStatement(sql)) {
                if (statementTimeoutSeconds > 0) ps.setQueryTimeout(statementTimeoutSeconds);

                while (it.hasNext()) {
                    T e = it.next();
                    Object[] params = meta.upsertByIdParams(e);

                    Jdbc.bind(ps, params, types);
                    ps.addBatch();
                    pending++;

                    if (pending >= batchSize) {
                        affected += sumBatch(ps.executeBatch());
                        pending = 0;
                    }
                }

                if (pending > 0) {
                    affected += sumBatch(ps.executeBatch());
                }
            }

            return affected;
        }

        private int batchInsertTemp(Connection con, Iterator<T> it) throws SQLException {
            final String insSql = meta.insertUpsertTempSql(dialect, suffix);

            // Temp rows have the same shape as upsertById (mergeCols).
            final SQLType[] types = meta.upsertByIdParamTypes();

            int inserted = 0;
            int pending = 0;

            try (PreparedStatement ps = con.prepareStatement(insSql)) {
                if (statementTimeoutSeconds > 0) ps.setQueryTimeout(statementTimeoutSeconds);

                while (it.hasNext()) {
                    T e = it.next();
                    Object[] params = meta.upsertByIdParams(e);

                    Jdbc.bind(ps, params, types);
                    ps.addBatch();
                    pending++;

                    if (pending >= batchSize) {
                        inserted += sumBatch(ps.executeBatch());
                        pending = 0;
                    }
                }

                if (pending > 0) {
                    inserted += sumBatch(ps.executeBatch());
                }
            }

            return inserted;
        }

        private void execSql(Connection con, String sql, boolean isCreate) throws SQLException {
            try (Statement st = con.createStatement()) {
                if (statementTimeoutSeconds > 0) st.setQueryTimeout(statementTimeoutSeconds);
                st.execute(sql);
            } catch (SQLException e) {
                if (isCreate && ignoreAlreadyExistsOnCreate && looksLikeAlreadyExists(e)) {
                    return; // ignore
                }
                throw e;
            }
        }

        private int execUpdate(Connection con, String sql) throws SQLException {
            try (Statement st = con.createStatement()) {
                if (statementTimeoutSeconds > 0) st.setQueryTimeout(statementTimeoutSeconds);
                return st.executeUpdate(sql);
            }
        }

        private int sumBatch(int[] counts) {
            if (counts == null) return 0;
            int sum = 0;
            for (int c : counts) {
                // JDBC can return SUCCESS_NO_INFO (-2). Count it as 1 so callers get some signal.
                if (c == Statement.SUCCESS_NO_INFO) sum += 1;
                else if (c > 0) sum += c;
            }
            return sum;
        }

        private boolean looksLikeAlreadyExists(SQLException e) {
            String msg = (e.getMessage() == null) ? "" : e.getMessage().toLowerCase(java.util.Locale.ROOT);
            String state = (e.getSQLState() == null) ? "" : e.getSQLState();

            // Oracle: ORA-00955
            if (msg.contains("ora-00955")) return true;

            // Generic
            if (msg.contains("already exists") || msg.contains("name is already used") || msg.contains("duplicate")) return true;

            // HSQL often uses 42504 for "object name already exists" (driver-dependent).
            if ("42504".equals(state)) return true;

            return false;
        }

        /**
         * Extract table name from SQL of the form:
         *   INSERT INTO <table> ( ... ) VALUES ...
         */
        private static String parseInsertIntoTableName(String insertSql) {
            if (insertSql == null) throw new IllegalArgumentException("insertSql is null");
            String s = insertSql.trim();

            // Case-insensitive prefix check
            String up = s.toUpperCase(java.util.Locale.ROOT);
            int p = up.indexOf("INSERT INTO ");
            if (p < 0) throw new IllegalArgumentException("Not an INSERT INTO SQL: " + insertSql);

            int start = p + "INSERT INTO ".length();
            // next whitespace or '('
            int end = start;
            while (end < s.length()) {
                char ch = s.charAt(end);
                if (Character.isWhitespace(ch) || ch == '(') break;
                end++;
            }
            String tn = s.substring(start, end).trim();
            if (tn.isEmpty()) throw new IllegalArgumentException("Could not parse INSERT INTO table name: " + insertSql);
            return tn;
        }
    }
}
