package org.github.dbjo.meta.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

final class JdbcBatchCountsTest {

    @Test
    void analyzeHandlesNull() {
        var info = Jdbc.analyzeBatchCounts(null);
        assertEquals(0, info.sum());
        assertEquals(0, info.successNoInfoCount());
        assertEquals(0, info.failedCount());
    }

    @Test
    void analyzeCountsSuccessNoInfoAsOne() {
        int[] counts = { Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO };
        var info = Jdbc.analyzeBatchCounts(counts);
        assertEquals(2, info.sum());
        assertEquals(2, info.successNoInfoCount());
        assertEquals(0, info.failedCount());
    }

    @Test
    void analyzeCountsFailuresSeparately() {
        int[] counts = { Statement.EXECUTE_FAILED, 3, Statement.SUCCESS_NO_INFO };
        var info = Jdbc.analyzeBatchCounts(counts);
        assertEquals(4, info.sum());
        assertEquals(1, info.successNoInfoCount());
        assertEquals(1, info.failedCount());
    }

    @Test
    void sumBatchCountsMatchesAnalyzeSum() {
        int[] counts = { 1, Statement.SUCCESS_NO_INFO, 5 };
        assertEquals(Jdbc.analyzeBatchCounts(counts).sum(), Jdbc.sumBatchCounts(counts));
    }
}
