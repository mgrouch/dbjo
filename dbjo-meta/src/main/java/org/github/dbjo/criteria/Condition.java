package org.github.dbjo.criteria;

import java.io.Serializable;

public sealed interface Condition<B extends Serializable>
        permits TrueCond, FalseCond,
        Eq, Ne, In, IsNull, IsNotNull,
        Between, Cmp,
        And, Or, Not {

    default Condition<B> and(Condition<B> other) { return Conditions.and(this, other); }
    default Condition<B> or(Condition<B> other)  { return Conditions.or(this, other); }
    default Condition<B> not() { return new Not<>(this); }
}
