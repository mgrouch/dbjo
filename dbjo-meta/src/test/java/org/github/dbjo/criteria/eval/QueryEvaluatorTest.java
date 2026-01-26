package org.github.dbjo.criteria.eval;

import org.github.dbjo.criteria.*;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class QueryEvaluatorTest {

    record Foo(Integer id, String name) implements Serializable {}

    private static final PropertyMeta<Foo, Integer> ID = new PropertyMeta<>(
            "id",
            Integer.class,
            Foo::id,
            (b, v) -> { /* unused */ }
    );

    private static final PropertyMeta<Foo, String> NAME = new PropertyMeta<>(
            "name",
            String.class,
            Foo::name,
            (b, v) -> { /* unused */ }
    );

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final EntityMeta<Foo> META = new EntityMeta<>(
            (List) List.of(ID, NAME),
            List.of("id", "name"),
            List.of(Integer.class, String.class)
    );

    @Test
    void whereOnly_works() {
        var q = Query.from(META)
                .where(Conditions.eq(ID, 2))
                .build();

        assertTrue(QueryEvaluator.test(q, new Foo(2, "a")));
        assertFalse(QueryEvaluator.test(q, new Foo(3, "a")));
    }

    @Test
    void scanRange_isAppliedWithBounds() {
        var q = Query.from(META)
                .scan(ID, Range.closedOpen(2, 4)) // 2 <= id < 4
                .build();

        assertFalse(QueryEvaluator.test(q, new Foo(1, "a")));
        assertTrue(QueryEvaluator.test(q, new Foo(2, "a")));
        assertTrue(QueryEvaluator.test(q, new Foo(3, "a")));
        assertFalse(QueryEvaluator.test(q, new Foo(4, "a")));
    }

    @Test
    void whereAndScan_bothMustMatch() {
        var q = Query.from(META)
                .scan(ID, Range.closedOpen(0, 10))
                .where(Conditions.eq(NAME, "ok"))
                .build();

        assertTrue(QueryEvaluator.test(q, new Foo(5, "ok")));
        assertFalse(QueryEvaluator.test(q, new Foo(5, "no")));
        assertFalse(QueryEvaluator.test(q, new Foo(11, "ok")));
    }
}
