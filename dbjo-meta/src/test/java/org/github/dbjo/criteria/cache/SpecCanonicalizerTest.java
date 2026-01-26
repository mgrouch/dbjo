package org.github.dbjo.criteria.cache;

import org.github.dbjo.criteria.spec.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class SpecCanonicalizerTest {

    @Test
    void eqNullBecomesIsNull() {
        QuerySpec q = new QuerySpec("T", new EqSpec("a", null), null, null);
        QuerySpec c = SpecCanonicalizer.canonicalize(q);

        assertInstanceOf(IsNullSpec.class, c.where());
        IsNullSpec isNull = (IsNullSpec) c.where();
        assertEquals("a", isNull.property());
    }

    @Test
    void andIsFlattenedSortedAndTrueRemoved() {
        CondSpec w = new AndSpec(List.of(
                new EqSpec("b", 1),
                new TrueSpec(),
                new AndSpec(List.of(new EqSpec("a", 1)))
        ));

        QuerySpec q = new QuerySpec("T", w, null, null);
        QuerySpec c = SpecCanonicalizer.canonicalize(q);

        assertInstanceOf(AndSpec.class, c.where());
        AndSpec a = (AndSpec) c.where();
        assertEquals(2, a.items().size());

        assertInstanceOf(EqSpec.class, a.items().get(0));
        assertEquals("a", ((EqSpec) a.items().get(0)).property());

        assertInstanceOf(EqSpec.class, a.items().get(1));
        assertEquals("b", ((EqSpec) a.items().get(1)).property());
    }

    @Test
    void inIsDedupedAndSorted() {
        QuerySpec q = new QuerySpec("T", new InSpec("x", List.of(3, 1, 2, 2)), null, null);
        QuerySpec c = SpecCanonicalizer.canonicalize(q);

        assertInstanceOf(InSpec.class, c.where());
        InSpec in = (InSpec) c.where();
        assertEquals(List.of(1, 2, 3), in.values());
    }
}
