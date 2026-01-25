package org.github.dbjo.dao.jdbc;

import org.github.dbjo.meta.jdbc.*;

import javax.sql.DataSource;
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
