package org.github.dbjo.meta.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Objects;

/**
 * Batch upsert helper using DbMeta's generated temp-table + merge SQL.
 *
 * Typical flow:
 *  1) (optional) CREATE temp table
 *  2) INSERT rows into temp table (JDBC batch)
 *  3) MERGE temp -> target
 *  4) (optional) DROP temp table
 */
public final class BatchUpsert {
    private BatchUpsert() {}

    public record Result(int rowsInsertedIntoTemp, int mergeAffectedRows) {}

    public static <T> Builder<T> forMeta(DbMeta<T> meta) {
        return new Builder<>(meta);
    }

    public static final class Builder<T> {
        private final DbMeta<T> meta;

        private DbDialect dialect;
        private String suffix = "X";

        private int batchSize = 500;

        /** Attempt to create temp table. For ORACLE/HSQL this may be a schema object; "already exists" can be ignored. */
        private boolean ensureTempTable = true;

        /** If true, do DELETE FROM temp before loading (useful if you use ON COMMIT PRESERVE ROWS). */
        private boolean clearTempBeforeLoad = false;

        /**
         * Drop temp table at end.
         * Default behavior:
         *  - MSSQL/SYBASE: true (session temp)
         *  - ORACLE/HSQL:  false (GTT is a schema object; dropping is heavy)
         */
        private Boolean dropTempTable = null;

        /** If true, ignore create-table errors that look like "already exists". */
        private boolean ignoreAlreadyExistsOnCreate = true;

        /** Optional statement timeout (0 = driver default). */
        private int statementTimeoutSeconds = 0;

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

        public Result execute(Connection con, Iterable<T> rows) throws SQLException {
            Objects.requireNonNull(con, "con");
            Objects.requireNonNull(rows, "rows");
            if (dialect == null) throw new IllegalStateException("dialect not set");

            Iterator<T> it = rows.iterator();
            if (!it.hasNext()) return new Result(0, 0);

            boolean originalAutoCommit = con.getAutoCommit();
            boolean needsTxnForTempRows = (dialect == DbDialect.ORACLE || dialect == DbDialect.HSQL);

            // IMPORTANT:
            // If the generated GTT uses "ON COMMIT DELETE ROWS", autoCommit=true would clear temp rows
            // between insert and merge. We force a transaction for ORACLE/HSQL if needed.
            if (needsTxnForTempRows && originalAutoCommit) {
                con.setAutoCommit(false);
            }

            boolean drop = (dropTempTable != null)
                    ? dropTempTable
                    : (dialect == DbDialect.MSSQL || dialect == DbDialect.SYBASE);

            int inserted = 0;
            int merged;

            try {
                if (ensureTempTable) {
                    String ddl = meta.createUpsertTempTableSql(dialect, suffix);
                    execDdl(con, ddl, true);
                }

                if (clearTempBeforeLoad) {
                    String tn = tempNameForDelete(meta, dialect, suffix);
                    execDdl(con, "DELETE FROM " + tn, false);
                }

                inserted = batchInsertTemp(con, it);

                // Merge temp -> target
                String mergeSql = meta.mergeUpsertFromTempSql(dialect, suffix);
                merged = execUpdate(con, mergeSql);

                if (drop) {
                    String dropSql = meta.dropUpsertTempTableSql(dialect, suffix);
                    execDdl(con, dropSql, false);
                }

                if (needsTxnForTempRows && originalAutoCommit) {
                    con.commit();
                }

                return new Result(inserted, merged);
            } catch (SQLException e) {
                if (needsTxnForTempRows && originalAutoCommit) {
                    try { con.rollback(); } catch (SQLException ignored) {}
                }
                throw e;
            } finally {
                if (needsTxnForTempRows && originalAutoCommit) {
                    try { con.setAutoCommit(true); } catch (SQLException ignored) {}
                }
            }
        }

        private int batchInsertTemp(Connection con, Iterator<T> it) throws SQLException {
            String insSql = meta.insertUpsertTempSql(dialect, suffix);
            SQLType[] types = meta.upsertTempParamTypes();

            int inserted = 0;
            int pending = 0;

            try (PreparedStatement ps = con.prepareStatement(insSql)) {
                if (statementTimeoutSeconds > 0) ps.setQueryTimeout(statementTimeoutSeconds);

                while (it.hasNext()) {
                    T e = it.next();
                    Object[] params = meta.upsertTempParams(e);

                    // Uses your runtime binder:
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

        private void execDdl(Connection con, String sql, boolean isCreate) throws SQLException {
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

        private static String tempNameForDelete(DbMeta<?> meta, DbDialect dialect, String suffix) {
            // We do not have direct access to the generated private upsertTempName().
            // So we reconstruct the convention used in the generator:
            //  - MSSQL/SYBASE: "#"+TABLE+"_UPSERT_"+suffix
            //  - ORACLE/HSQL :  TABLE+"_UPSERT_"+suffix
            String t = meta.table();
            String sfx = (suffix == null || suffix.isBlank()) ? "X" : suffix.trim();
            return switch (dialect) {
                case MSSQL, SYBASE -> "#" + t + "_UPSERT_" + sfx;
                case ORACLE, HSQL  -> t + "_UPSERT_" + sfx;
            };
        }
    }
}
