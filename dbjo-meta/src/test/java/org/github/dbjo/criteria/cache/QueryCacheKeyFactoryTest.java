package org.github.dbjo.criteria.cache;

import org.github.dbjo.criteria.spec.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for stable/canonical query cache keys.
 */
final class QueryCacheKeyFactoryTest {

    @Test
    void andItemOrder_doesNotChangeCacheKey() {
        QuerySpec a = new QuerySpec(
                "E",
                new AndSpec(List.of(
                        new EqSpec("id", 1),
                        new EqSpec("name", "a")
                )),
                null,
                10
        );

        QuerySpec b = new QuerySpec(
                "E",
                new AndSpec(List.of(
                        new EqSpec("name", "a"),
                        new EqSpec("id", 1)
                )),
                null,
                10
        );

        assertEquals(QueryCacheKeyFactory.from(a), QueryCacheKeyFactory.from(b));
    }

    @Test
    void eqNull_isCanonicalizedToIsNull() {
        QuerySpec a = new QuerySpec("E", new EqSpec("createdAt", null), null, null);
        QuerySpec b = new QuerySpec("E", new IsNullSpec("createdAt"), null, null);

        assertEquals(QueryCacheKeyFactory.from(a), QueryCacheKeyFactory.from(b));
    }

    @Test
    void inValueOrder_doesNotChangeCacheKey() {
        QuerySpec a = new QuerySpec("E", new InSpec("id", List.of(3, 1, 2)), null, null);
        QuerySpec b = new QuerySpec("E", new InSpec("id", List.of(1, 2, 3)), null, null);

        assertEquals(QueryCacheKeyFactory.from(a), QueryCacheKeyFactory.from(b));
    }

    @Test
    void scanBoundsAreUppercasedForStability() {
        QuerySpec a = new QuerySpec(
                "E",
                null,
                new ScanSpec("id", new RangeSpec(1, "inclusive", 10, "exclusive")),
                null
        );
        QuerySpec b = new QuerySpec(
                "E",
                null,
                new ScanSpec("id", new RangeSpec(1, "INCLUSIVE", 10, "EXCLUSIVE")),
                null
        );

        assertEquals(QueryCacheKeyFactory.from(a), QueryCacheKeyFactory.from(b));
    }
}
