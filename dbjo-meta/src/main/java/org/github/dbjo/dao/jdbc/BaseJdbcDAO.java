package org.github.dbjo.dao.jdbc;

import org.github.dbjo.criteria.Query;
import org.github.dbjo.criteria.eval.QueryEvaluator;
import org.github.dbjo.criteria.sql.SqlCriteriaCompiler;
import org.github.dbjo.meta.jdbc.*;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public abstract class BaseJdbcDAO<T, K> {
    private static final Pattern WHERE_PATTERN = Pattern.compile("\\bwhere\\b", Pattern.CASE_INSENSITIVE);
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
        try (PreparedStatement ps = c.prepareStatement(meta.selectAllBaseSql());
             ResultSet rs = ps.executeQuery()) {

            List<T> out = new ArrayList<>();
            while (rs.next()) out.add(meta.fromRow(rs));
            return out;
        }
    }

    public List<T> selectAllByPartition(int partitionNum, int totalPartitions) throws SQLException {
        try (Connection c = ds.getConnection()) {
            return selectAllByPartition(c, partitionNum, totalPartitions);
        }
    }

    public List<T> selectAllByPartition(int partitionNum, int totalPartitions, String additionalCriteria) throws SQLException {
        try (Connection c = ds.getConnection()) {
            return selectAllByPartition(c, partitionNum, totalPartitions, additionalCriteria);
        }
    }

    public List<T> selectAllByPartition(Connection c, int partitionNum, int totalPartitions) throws SQLException {
        return selectAllByPartition(c, partitionNum, totalPartitions, null);
    }

    public List<T> selectAllByPartition(Connection c, int partitionNum, int totalPartitions, String additionalCriteria)
            throws SQLException {
        String sql = withPartitionPredicate(meta.selectAllBaseSql());
        sql = appendAdditionalCriteria(sql, additionalCriteria);
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setInt(1, totalPartitions);
            ps.setInt(2, partitionNum);
            try (ResultSet rs = ps.executeQuery()) {
                List<T> out = new ArrayList<>();
                while (rs.next()) out.add(meta.fromRow(rs));
                return out;
            }
        }
    }

    private static String withPartitionPredicate(String sql) {
        String trimmed = sql.trim();
        String predicate = "partition_id(partition_key, ?) = ?";
        if (WHERE_PATTERN.matcher(trimmed).find()) {
            return trimmed + " AND " + predicate;
        }
        return trimmed + " WHERE " + predicate;
    }

    private static String appendAdditionalCriteria(String sql, String additionalCriteria) {
        if (additionalCriteria == null || additionalCriteria.isBlank()) {
            return sql;
        }
        return sql + " AND " + additionalCriteria.trim();
    }

    // Criteria -> SQL bridge (property-name based)

    public List<T> select(Query<? extends Serializable> q) throws SQLException {
        try (Connection c = ds.getConnection()) {
            return select(c, q);
        }
    }

    public List<T> select(Connection c, Query<? extends Serializable> q) throws SQLException {
        Objects.requireNonNull(c, "c");
        Objects.requireNonNull(q, "q");

        Integer limObj = q.limit();
        int limit = (limObj == null) ? Integer.MAX_VALUE : Math.max(0, limObj);
        if (limit == 0) return List.of();

        final SqlCriteriaCompiler.Compiled compiled;
        try {
            compiled = SqlCriteriaCompiler.compileSelectAll(meta, q);
        } catch (IllegalArgumentException unsupported) {
            return selectAllAndFilterInMemory(c, q, limit);
        }

        List<T> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(compiled.sql())) {
            ps.setMaxRows(limit);
            Jdbc.bind(ps, compiled.params(), null);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(meta.fromRow(rs));
                    if (out.size() >= limit) break; // safety
                }
            }
        }
        return out;
    }

    private List<T> selectAllAndFilterInMemory(Connection c, Query<? extends Serializable> q, int limit) throws SQLException {
        List<T> out = new ArrayList<>();

        try (PreparedStatement ps = c.prepareStatement(meta.selectAllBaseSql());
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                T e = meta.fromRow(rs);
                if (QueryEvaluator.testRaw(q, e)) {
                    out.add(e);
                    if (out.size() >= limit) break;
                }
            }
        }
        return out;
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
