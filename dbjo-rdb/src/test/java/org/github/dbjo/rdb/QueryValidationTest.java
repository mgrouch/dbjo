package org.github.dbjo.rdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class QueryValidationTest {

    @Test
    void onlyOneIndexPredicateIsSupported() {
        var b = Query.<Long>builder()
                .where(new IndexPredicate.Eq("idx1", new byte[] { 1 }))
                .where(new IndexPredicate.Eq("idx2", new byte[] { 2 }));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, b::build);
        assertTrue(ex.getMessage().contains("Only one index predicate"));
    }
}
