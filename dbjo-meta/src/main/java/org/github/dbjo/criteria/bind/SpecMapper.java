package org.github.dbjo.criteria.bind;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.github.dbjo.criteria.*;
import org.github.dbjo.criteria.spec.*;

public final class SpecMapper {
    private SpecMapper() {}

    public static <B extends Serializable> CondSpec toSpec(Condition<B> c) {
        if (c instanceof TrueCond) return new TrueSpec();
        if (c instanceof FalseCond) return new FalseSpec();

        if (c instanceof And<B> a) return new AndSpec(List.of(toSpec(a.left()), toSpec(a.right())));
        if (c instanceof Or<B> o)  return new OrSpec(List.of(toSpec(o.left()), toSpec(o.right())));
        if (c instanceof Not<B> n) return new NotSpec(toSpec(n.inner()));

        if (c instanceof IsNull<B, ?> x) return new IsNullSpec(x.prop().getPropertyName());
        if (c instanceof IsNotNull<B, ?> x) return new IsNotNullSpec(x.prop().getPropertyName());

        if (c instanceof Eq<B, ?> x) return new EqSpec(x.prop().getPropertyName(), x.value());
        if (c instanceof Ne<B, ?> x) return new NeSpec(x.prop().getPropertyName(), x.value());
        if (c instanceof In<B, ?> x) return new InSpec(x.prop().getPropertyName(), new ArrayList<>(x.values()));
        if (c instanceof Between<B, ?> x) return new BetweenSpec(x.prop().getPropertyName(), x.lo(), x.hi());
        if (c instanceof Cmp<B, ?> x) return new CmpSpec(x.prop().getPropertyName(), x.op().name(), x.value());

        throw new IllegalArgumentException("Unsupported condition node: " + c.getClass().getName());
    }
}
