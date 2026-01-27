package org.github.dbjo.criteria.eval;

import org.github.dbjo.criteria.*;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ConditionEvaluator} semantics.
 */
final class ConditionEvaluatorMoreTest {

    public static final class Foo implements Serializable {
        private Integer n;
        private String s;
        public Foo(Integer n, String s) { this.n = n; this.s = s; }
        public Integer getN() { return n; }
        public void setN(Integer n) { this.n = n; }
        public String getS() { return s; }
        public void setS(String s) { this.s = s; }
    }

    private static final PropertyMeta<Foo, Integer> N =
            new PropertyMeta<>("n", Integer.class, Foo::getN, Foo::setN);

    private static final PropertyMeta<Foo, String> S =
            new PropertyMeta<>("s", String.class, Foo::getS, Foo::setS);

    @Test
    void in_returnsFalseOnNullProperty() {
        Foo foo = new Foo(null, null);
        Condition<Foo> c = new In<>(N, List.of(1, 2, 3));
        assertFalse(ConditionEvaluator.test(c, foo));
    }

    @Test
    void between_isInclusiveOnBothEnds() {
        Foo foo = new Foo(10, null);
        assertTrue(ConditionEvaluator.test(Conditions.between(N, 10, 12), foo));
        assertTrue(ConditionEvaluator.test(Conditions.between(N, 8, 10), foo));
        assertFalse(ConditionEvaluator.test(Conditions.between(N, 11, 12), foo));
    }

    @Test
    void cmp_returnsFalseOnNullProperty() {
        Foo foo = new Foo(null, null);
        assertFalse(ConditionEvaluator.test(Conditions.gt(N, 1), foo));
    }

    @Test
    void eqNull_matchesNullProperty() {
        Foo foo = new Foo(null, null);
        assertTrue(ConditionEvaluator.test(Conditions.eq(S, null), foo));
        assertFalse(ConditionEvaluator.test(Conditions.ne(S, null), foo));
    }
}
