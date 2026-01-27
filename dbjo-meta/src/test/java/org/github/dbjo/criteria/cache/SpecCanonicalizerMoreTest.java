package org.github.dbjo.criteria.cache;

import org.github.dbjo.criteria.spec.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link SpecCanonicalizer} simplifications.
 */
final class SpecCanonicalizerMoreTest {

    @Test
    void and_identities_and_shortCircuit() {
        QuerySpec q = new QuerySpec("E",
                new AndSpec(List.of(new TrueSpec(), new EqSpec("id", 1))),
                null,
                null
        );
        QuerySpec c = SpecCanonicalizer.canonicalize(q);
        assertTrue(c.where() instanceof EqSpec);

        QuerySpec q2 = new QuerySpec("E",
                new AndSpec(List.of(new FalseSpec(), new EqSpec("id", 1))),
                null,
                null
        );
        QuerySpec c2 = SpecCanonicalizer.canonicalize(q2);
        assertTrue(c2.where() instanceof FalseSpec);
    }

    @Test
    void or_identities_and_shortCircuit() {
        QuerySpec q = new QuerySpec("E",
                new OrSpec(List.of(new FalseSpec(), new EqSpec("id", 1))),
                null,
                null
        );
        QuerySpec c = SpecCanonicalizer.canonicalize(q);
        assertTrue(c.where() instanceof EqSpec);

        QuerySpec q2 = new QuerySpec("E",
                new OrSpec(List.of(new TrueSpec(), new EqSpec("id", 1))),
                null,
                null
        );
        QuerySpec c2 = SpecCanonicalizer.canonicalize(q2);
        assertTrue(c2.where() instanceof TrueSpec);
    }

    @Test
    void badScanBoundToken_throws() {
        QuerySpec q = new QuerySpec(
                "E",
                null,
                new ScanSpec("id", new RangeSpec(1, "BAD", 2, "INCLUSIVE")),
                null
        );

        assertThrows(IllegalArgumentException.class, () -> SpecCanonicalizer.canonicalize(q));
    }
}
