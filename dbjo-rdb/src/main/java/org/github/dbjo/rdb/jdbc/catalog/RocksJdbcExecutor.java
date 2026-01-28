package org.github.dbjo.rdb.jdbc.catalog;

import org.github.dbjo.rdb.jdbc.RocksJdbcConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public final class RocksJdbcExecutor {
    private RocksJdbcExecutor() {}

    public static ResultSet execute(RocksJdbcConnection conn, String sql, int maxRows) throws SQLException {
        Objects.requireNonNull(conn, "conn");
        Objects.requireNonNull(sql, "sql");

        RocksJdbcSql.Parsed parsed = RocksJdbcSql.parse(sql);
        RocksJdbcCatalog catalog = conn.rocksCatalog();

        RocksJdbcPlan plan;
        if (parsed.kind() == RocksJdbcSql.Kind.LIST_TABLES) {
            plan = new RocksJdbcPlan.ListTables(cap(parsed.limit(), maxRows));
        } else {
            RocksJdbcTableMeta meta = catalog.tableMeta(parsed.tableName());
            plan = RocksJdbcPlanner.plan(parsed, meta);
            plan = capLimit(plan, maxRows);
        }

        return catalog.execute(plan);
    }

    private static Integer cap(Integer lim, int maxRows) {
        if (maxRows <= 0) return lim;
        if (lim == null) return maxRows;
        return Math.min(lim, maxRows);
    }

    private static RocksJdbcPlan capLimit(RocksJdbcPlan plan, int maxRows) {
        if (maxRows <= 0) return plan;

        if (plan instanceof RocksJdbcPlan.ListTables lt) {
            return new RocksJdbcPlan.ListTables(cap(lt.limit(), maxRows));
        }
        if (plan instanceof RocksJdbcPlan.Select s) {
            return new RocksJdbcPlan.Select(s.table(), s.projection(), cap(s.limit(), maxRows), s.accessPath(), s.whereSql());
        }
        if (plan instanceof RocksJdbcPlan.Count c) {
            return new RocksJdbcPlan.Count(c.table(), cap(c.limit(), maxRows), c.accessPath(), c.whereSql());
        }
        return plan;
    }
}
