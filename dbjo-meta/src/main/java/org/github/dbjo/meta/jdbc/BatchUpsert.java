package org.github.dbjo.meta.jdbc;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Objects;

/**
 * Dialect-aware batch upsert:
 *  - MSSQL/Sybase: temp-table + single MERGE (preferred)
 *  - Oracle/HSQL: direct PreparedStatement batching of upsertByIdSql(dialect)
 */
public final class BatchUpsert<T> implements AutoCloseable {

    /**
     * Detailed counters from a flush().
     * For temp-table strategies: rowsInsertedIntoTemp + mergeAffectedRows.
     * For direct strategies: rowsInsertedIntoTemp=0 and mergeAffectedRows=batchAffectedRows.
     */
    public record Result(int rowsInsertedIntoTemp, int mergeAffectedRows) {
        public int total() { return rowsInsertedIntoTemp + mergeAffectedRows; }
    }

    public static <T> Builder<T> builder(DataSource ds, DbDialect dialect, DbMetaUpsertSupport<T> meta) {
        return new Builder<>(ds, dialect, meta);
    }

    public static final class Builder<T> {
        private final DataSource ds;
        private final DbDialect dialect;
        private final DbMetaUpsertSupport<T> meta;

        private String suffix = "X";
        private int batchSize = 500;
        private boolean dropTempOnClose = true;
        private Connection externalConn;

        private Builder(DataSource ds, DbDialect dialect, DbMetaUpsertSupport<T> meta) {
            this.ds = Objects.requireNonNull(ds, "ds");
            this.dialect = Objects.requireNonNull(dialect, "dialect");
            this.meta = Objects.requireNonNull(meta, "meta");
        }

        public Builder<T> suffix(String suffix) { this.suffix = suffix; return this; }
        public Builder<T> batchSize(int batchSize) { this.batchSize = Math.max(1, batchSize); return this; }
        public Builder<T> dropTempOnClose(boolean drop) { this.dropTempOnClose = drop; return this; }

        /** Use caller-managed connection (BatchUpsert won't close it). */
        public Builder<T> connection(Connection conn) { this.externalConn = conn; return this; }

        public BatchUpsert<T> open() throws SQLException {
            Connection c = (externalConn != null) ? externalConn : ds.getConnection();
            boolean owns = (externalConn == null);
            return new BatchUpsert<>(c, owns, dialect, meta, suffix, batchSize, dropTempOnClose);
        }
    }

    private final Connection conn;
    private final boolean ownsConn;
    private final DbDialect dialect;
    private final DbMetaUpsertSupport<T> meta;

    private final String suffix;
    private final int batchSize;
    private final boolean dropTempOnClose;

    private final boolean useTemp;

    private PreparedStatement psDirect;

    private PreparedStatement psTempIns;
    private Statement stTempCtl;

    private int queued = 0;

    private BatchUpsert(Connection conn,
                        boolean ownsConn,
                        DbDialect dialect,
                        DbMetaUpsertSupport<T> meta,
                        String suffix,
                        int batchSize,
                        boolean dropTempOnClose) {
        this.conn = Objects.requireNonNull(conn, "conn");
        this.ownsConn = ownsConn;
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.meta = Objects.requireNonNull(meta, "meta");
        this.suffix = suffix;
        this.batchSize = batchSize;
        this.dropTempOnClose = dropTempOnClose;

        this.useTemp = meta.supportsUpsertTemp(dialect);
    }

    public BatchUpsert<T> add(T row) throws SQLException {
        if (useTemp) {
            ensureTemp();
            Jdbc.bind(psTempIns, meta.upsertByIdParams(row), meta.upsertByIdParamTypes());
            psTempIns.addBatch();
        } else {
            ensureDirect();
            Jdbc.bind(psDirect, meta.upsertByIdParams(row), meta.upsertByIdParamTypes());
            psDirect.addBatch();
        }

        queued++;
        if (queued >= batchSize) flush();
        return this;
    }

    public Result flushResult() throws SQLException {
        if (queued == 0) return new Result(0, 0);

        if (useTemp) {
            Jdbc.BatchCountInfo insInfo = Jdbc.analyzeBatchCounts(psTempIns.executeBatch());
            int inserted = insInfo.sum();

            int merged = stTempCtl.executeUpdate(meta.mergeUpsertFromTempSql(dialect, suffix));
            if (merged < 0) merged = 0;

            String tn = meta.upsertTempTableName(dialect, suffix);
            stTempCtl.executeUpdate("DELETE FROM " + tn);

            queued = 0;
            return new Result(inserted, merged);
        }

        Jdbc.BatchCountInfo directInfo = Jdbc.analyzeBatchCounts(psDirect.executeBatch());
        int directAffected = directInfo.sum();

        queued = 0;
        return new Result(0, directAffected);
    }

    public int flush() throws SQLException {
        return flushResult().total();
    }

    @Override
    public void close() throws SQLException {
        SQLException err = null;

        try {
            flush();
        } catch (SQLException e) {
            err = e;
        }

        if (useTemp && dropTempOnClose) {
            try {
                // Avoid double-close when stTempCtl is reused (some drivers may throw on 2nd close).
                if (stTempCtl != null) {
                    stTempCtl.executeUpdate(meta.dropUpsertTempTableSql(dialect, suffix));
                } else {
                    try (Statement st = conn.createStatement()) {
                        st.executeUpdate(meta.dropUpsertTempTableSql(dialect, suffix));
                    }
                }
            } catch (SQLException ignore) {
            }
        }

        tryClose(psDirect);
        tryClose(psTempIns);
        tryClose(stTempCtl);

        if (ownsConn) {
            try {
                conn.close();
            } catch (SQLException e) {
                if (err == null) err = e;
            }
        }

        if (err != null) throw err;
    }

    private void ensureDirect() throws SQLException {
        if (psDirect != null) return;
        psDirect = conn.prepareStatement(meta.upsertByIdSql(dialect));
    }

    private void ensureTemp() throws SQLException {
        if (psTempIns != null) return;

        stTempCtl = conn.createStatement();

        try { stTempCtl.executeUpdate(meta.dropUpsertTempTableSql(dialect, suffix)); }
        catch (SQLException ignore) {}

        stTempCtl.executeUpdate(meta.createUpsertTempTableSql(dialect, suffix));

        psTempIns = conn.prepareStatement(meta.insertUpsertTempSql(dialect, suffix));
    }

    private static void tryClose(AutoCloseable c) throws SQLException {
        if (c == null) return;
        try { c.close(); }
        catch (Exception e) {
            if (e instanceof SQLException se) throw se;
            throw new SQLException(e);
        }
    }
}
