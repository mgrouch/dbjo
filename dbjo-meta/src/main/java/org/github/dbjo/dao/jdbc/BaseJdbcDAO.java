package org.github.dbjo.dao.jdbc;

import org.github.dbjo.criteria.Query;
import org.github.dbjo.criteria.eval.QueryEvaluator;
import org.github.dbjo.meta.jdbc.*;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class BaseJdbcDAO<T, K> {
    protected final DataSource ds;
    protected final DbDialect dialect;
    protected final DbMeta<T> meta;

    protected BaseJdbcDAO(DataSource ds, DbDialect dialect, DbMeta<T> meta) {
        this.ds = Objects.requireNonNull(ds, "ds");
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.meta = Objects.requireNonNull(meta, "meta");
    }

    public DbDialect dialect() { return dialect; }
    public DbMeta<T> meta() { return meta; }

    public int insert(T row) throws SQLException {
        try (Connection c = ds.getConnection()) {
            return insert(c, row);
        }
    }

    public int insert(Connection c, T row) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(meta.insertSql())) {
            Jdbc.bind(ps, meta.insertParams(row), meta.insertParamTypes());
            return ps.executeUpdate();
        }
    }

    public int updateById(T row) throws SQLException {
        try (Connection c = ds.getConnection()) {
            return updateById(c, row);
        }
    }

    public int updateById(Connection c, T row) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(meta.updateByIdSql())) {
            Jdbc.bind(ps, meta.updateByIdParams(row), meta.updateByIdParamTypes());
            return ps.executeUpdate();
        }
    }

    public List<T> selectAll() throws SQLException {
        try (Connection c = ds.getConnection()) {
            return selectAll(c);
        }
    }

    public List<T> selectAll(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(meta.selectAllSql());
             ResultSet rs = ps.executeQuery()) {

            List<T> out = new ArrayList<>();
            while (rs.next()) out.add(meta.fromRow(rs));
            return out;
        }
    }

    public int upsertById(T row) throws SQLException {
        DbMetaUpsertSupport<T> um = requireUpsertMeta();
        try (Connection c = ds.getConnection()) {
            try (PreparedStatement ps = c.prepareStatement(um.upsertByIdSql(dialect))) {
                Jdbc.bind(ps, um.upsertByIdParams(row), um.upsertByIdParamTypes());
                return ps.executeUpdate();
            }
        }
    }

    /**
     * Execute a criteria {@link Query} by streaming {@link DbMeta#selectAllSql()} and
     * filtering in-memory.
     *
     * <p>Supports {@code where} + {@code scan} + {@code limit}. This method does not
     * currently attempt to compile the criteria into SQL (so it may be inefficient for
     * large tables), but it provides a single, consistent query surface that works for
     * both JDBC and Rocks runtimes.
     */
    public List<T> select(Query<? extends Serializable> q) throws SQLException {
        try (Connection c = ds.getConnection()) {
            return select(c, q);
        }
    }

    /** See {@link #select(Query)}. */
    public List<T> select(Connection c, Query<? extends Serializable> q) throws SQLException {
        Objects.requireNonNull(c, "c");
        Objects.requireNonNull(q, "q");

        final Integer limObj = q.limit();
        final int limit = (limObj == null) ? Integer.MAX_VALUE : Math.max(0, limObj);
        if (limit == 0) return List.of();

        List<T> rows = selectAll(c);
        List<T> out = new ArrayList<>(Math.min(limit, rows.size()));
        for (T row : rows) {
            if (!(row instanceof Serializable s)) {
                throw new IllegalStateException(
                        "Criteria queries require entities to implement java.io.Serializable. " +
                                "Entity class: " + (row == null ? "<null>" : row.getClass().getName()));
            }
            if (QueryEvaluator.test(q, s)) {
                out.add(row);
                if (out.size() >= limit) break;
            }
        }
        return out;
    }

    public BatchUpsert<T> batchUpsert() throws SQLException {
        DbMetaUpsertSupport<T> um = requireUpsertMeta();
        return BatchUpsert.builder(ds, dialect, um).open();
    }

    public BatchUpsert<T> batchUpsert(String suffix, int batchSize) throws SQLException {
        DbMetaUpsertSupport<T> um = requireUpsertMeta();
        return BatchUpsert.builder(ds, dialect, um).suffix(suffix).batchSize(batchSize).open();
    }

    @SuppressWarnings("unchecked")
    private DbMetaUpsertSupport<T> requireUpsertMeta() {
        if (meta instanceof DbMetaUpsertSupport<?> um) {
            return (DbMetaUpsertSupport<T>) um;
        }
        throw new IllegalStateException("Meta does not support upsert: " + meta.getClass().getName());
    }
}
