package org.github.dbjo.kafka.outbox.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.List;
import org.github.dbjo.meta.jdbc.DbDialect;
import org.github.dbjo.meta.jdbc.DbMeta;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboxSqlBuilderTest {
    private static final DbMeta<Object> META = new DbMeta<>() {
        @Override public String schema() { return "dbo"; }
        @Override public String table() { return "orders"; }
        @Override public String fqn() { return "dbo.orders"; }
        @Override public String insertSql() { return ""; }
        @Override public String updateByIdSql() { return ""; }
        @Override public String selectAllSql() { return "SELECT order_id, status, customer_id FROM dbo.orders"; }
        @Override public Object[] insertParams(Object e) { return new Object[0]; }
        @Override public SQLType[] insertParamTypes() { return new SQLType[0]; }
        @Override public Object[] updateByIdParams(Object e) { return new Object[0]; }
        @Override public SQLType[] updateByIdParamTypes() { return new SQLType[0]; }
        @Override public String upsertByIdSql(org.github.dbjo.meta.jdbc.DbDialect dialect) { return ""; }
        @Override public Object[] upsertByIdParams(Object e) { return new Object[0]; }
        @Override public SQLType[] upsertByIdParamTypes() { return new SQLType[0]; }
        @Override public Object fromRow(ResultSet rs) throws SQLException { return null; }
    };

    @Test
    void buildsCreateClaimAndUpdateSqlFromDbMeta() {
        OutboxSqlBuilder.OutboxSql sql = OutboxSqlBuilder.build(META, "dbo.order_outbox");

        assertEquals(List.of("order_id", "status", "customer_id"), sql.payloadColumns());
        assertTrue(sql.createTableSql().contains("SELECT TOP (0)"));
        assertTrue(sql.createTableSql().contains("INTO dbo.order_outbox"));
        assertTrue(sql.createTableSql().contains("FROM dbo.orders"));
        assertTrue(sql.createTableSql().contains("CREATE UNIQUE INDEX"));
        assertTrue(sql.createTableSql().contains("sequence_no"));

        assertTrue(sql.claimForUpdateSql().contains("WITH (ROWLOCK, UPDLOCK, READPAST)"));
        assertTrue(sql.claimForUpdateSql().contains("inserted.order_id"));

        assertTrue(sql.markPublishedSql().contains("UPDATE dbo.order_outbox"));
        assertTrue(sql.markPublishedSql().contains("WHERE outbox_id = :outboxId"));
    }

    @Test
    void parsesColumnsFromSelectSql() {
        List<String> columns = OutboxSqlBuilder.parseSelectColumns("SELECT a, b, c FROM t");
        assertEquals(List.of("a", "b", "c"), columns);
    }

    @Test
    void buildsSqlForOtherSupportedDialects() {
        OutboxSqlBuilder.OutboxSql hsqlSql = OutboxSqlBuilder.build(META, "dbo.order_outbox", DbDialect.HSQL);
        assertTrue(hsqlSql.createTableSql().contains("CREATE TABLE dbo.order_outbox AS"));
        assertTrue(hsqlSql.claimForUpdateSql().contains("FETCH FIRST :batchSize ROWS ONLY"));

        OutboxSqlBuilder.OutboxSql sybaseSql = OutboxSqlBuilder.build(META, "dbo.order_outbox", DbDialect.SYBASE);
        assertTrue(sybaseSql.createTableSql().contains("SELECT TOP (0)"));
        assertTrue(sybaseSql.claimForUpdateSql().contains("FETCH FIRST :batchSize ROWS ONLY"));

        OutboxSqlBuilder.OutboxSql oracleSql = OutboxSqlBuilder.build(META, "dbo.order_outbox", DbDialect.ORACLE);
        assertTrue(oracleSql.createTableSql().contains("CREATE TABLE dbo.order_outbox AS"));
        assertTrue(oracleSql.claimForUpdateSql().contains("FETCH FIRST :batchSize ROWS ONLY"));
    }
}
