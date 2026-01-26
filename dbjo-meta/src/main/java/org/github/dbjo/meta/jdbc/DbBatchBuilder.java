package org.github.dbjo.meta.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Iterator;

public final class DbBatchBuilder {
    private DbBatchBuilder() {}

    public static <T> int upsertAll(Connection c, DbDialect dialect, DbMeta<T> meta, Iterable<T> rows, String suffix)
            throws SQLException {

        if (c == null) throw new IllegalArgumentException("connection is null");
        if (dialect == null) throw new IllegalArgumentException("dialect is null");
        if (meta == null) throw new IllegalArgumentException("meta is null");
        if (rows == null) throw new IllegalArgumentException("rows is null");

        Iterator<T> it = rows.iterator();
        if (!it.hasNext()) return 0;

        if (meta.supportsUpsertTemp(dialect)) {
            return upsertViaTemp(c, dialect, meta, rows, suffix);
        }
        return upsertViaBatch(c, dialect, meta, rows);
    }

    private static <T> int upsertViaBatch(Connection c, DbDialect dialect, DbMeta<T> meta, Iterable<T> rows)
            throws SQLException {

        String sql = meta.upsertByIdSql(dialect);
        SQLType[] types = meta.upsertByIdParamTypes();

        int total = 0;
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (T e : rows) {
                Object[] params = meta.upsertByIdParams(e);
                Jdbc.bind(ps, params, types);
                ps.addBatch();
            }
            total += Jdbc.sumBatchCounts(ps.executeBatch());
        }
        return total;
    }

    private static <T> int upsertViaTemp(Connection c, DbDialect dialect, DbMeta<T> meta, Iterable<T> rows, String suffix)
            throws SQLException {

        String create = meta.createUpsertTempTableSql(dialect, suffix);
        String insert = meta.insertUpsertTempSql(dialect, suffix);
        String merge  = meta.mergeUpsertFromTempSql(dialect, suffix);
        String drop   = meta.dropUpsertTempTableSql(dialect, suffix);

        SQLType[] types = meta.upsertByIdParamTypes();

        int total = 0;
        try (PreparedStatement psCreate = c.prepareStatement(create)) {
            psCreate.execute();
        }

        try {
            try (PreparedStatement psIns = c.prepareStatement(insert)) {
                for (T e : rows) {
                    Object[] params = meta.upsertByIdParams(e);
                    Jdbc.bind(psIns, params, types);
                    psIns.addBatch();
                }
                total += Jdbc.sumBatchCounts(psIns.executeBatch());
            }

            try (PreparedStatement psMerge = c.prepareStatement(merge)) {
                int m = psMerge.executeUpdate();
                if (m > 0) total += m;
            }
        } finally {
            try (PreparedStatement psDrop = c.prepareStatement(drop)) {
                psDrop.execute();
            } catch (SQLException ignored) {
            }
        }

        return total;
    }
}
