package org.github.dbjo.criteria.bind;

import org.github.dbjo.criteria.Query;
import org.github.dbjo.criteria.eval.ConditionEvaluator;
import org.github.dbjo.criteria.spec.EqSpec;
import org.github.dbjo.criteria.spec.QuerySpec;
import org.github.dbjo.meta.entity.DefaultMetaRegistry;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.junit.jupiter.api.Test;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class QueryBinderTest {

    static final class Bean implements Serializable {
        @Serial private static final long serialVersionUID = 1L;

        private Integer id;

        Integer getId() { return id; }
        void setId(Integer id) { this.id = id; }
    }

    @Test
    void bindsStringToIntegerUsingPropertyType() {
        PropertyMeta<Bean, Integer> idMeta =
                new PropertyMeta<>("id", Integer.class, Bean::getId, Bean::setId);

        @SuppressWarnings({ "rawtypes", "unchecked" })
        EntityMeta<Bean> em = new EntityMeta<>(
                (List) List.of(idMeta),
                List.of("id"),
                List.of(Integer.class)
        );

        DefaultMetaRegistry reg = new DefaultMetaRegistry().register("T", em);

        QueryBinder qb = new QueryBinder(reg);

        QuerySpec spec = new QuerySpec("T", new EqSpec("id", "42"), null, null);
        Query<?> q = qb.fromSpec(spec);

        Bean b = new Bean();
        b.setId(42);

        // ConditionEvaluator works with the runtime bean instance even though Query<?> is raw here.
        @SuppressWarnings("unchecked")
        boolean ok = ConditionEvaluator.test(((Query<Bean>) q).where(), b);

        assertTrue(ok);
    }
}
