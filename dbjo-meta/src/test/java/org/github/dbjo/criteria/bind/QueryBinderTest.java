package org.github.dbjo.criteria.bind;

import org.github.dbjo.criteria.*;
import org.github.dbjo.criteria.eval.ConditionEvaluator;
import org.github.dbjo.criteria.spec.*;
import org.github.dbjo.meta.entity.DefaultMetaRegistry;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.junit.jupiter.api.Test;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class QueryBinderTest {

    enum Status {
        A(1), B(2);

        final int code;
        Status(int c) { this.code = c; }

        public static Status of(int c) {
            return switch (c) {
                case 1 -> A;
                case 2 -> B;
                default -> throw new IllegalArgumentException("bad code: " + c);
            };
        }

        public static Status ofNullable(Integer c) {
            return (c == null) ? null : of(c);
        }

        public static Status byCodeNullable(String s) {
            if (s == null || s.isBlank()) return null;
            return of(Integer.parseInt(s.trim()));
        }
    }

    static final class Bean implements Serializable {
        @Serial private static final long serialVersionUID = 1L;

        private Integer id;
        private Boolean active;
        private Status status;
        private String name;

        Integer getId() { return id; }
        void setId(Integer id) { this.id = id; }

        Boolean getActive() { return active; }
        void setActive(Boolean active) { this.active = active; }

        Status getStatus() { return status; }
        void setStatus(Status status) { this.status = status; }

        String getName() { return name; }
        void setName(String name) { this.name = name; }
    }

    private static DefaultMetaRegistry registry() {
        PropertyMeta<Bean, Integer> idMeta =
                new PropertyMeta<>("id", Integer.class, Bean::getId, Bean::setId);

        PropertyMeta<Bean, Boolean> activeMeta =
                new PropertyMeta<>("active", Boolean.class, Bean::getActive, Bean::setActive);

        PropertyMeta<Bean, Status> statusMeta =
                new PropertyMeta<>("status", Status.class, Bean::getStatus, Bean::setStatus);

        PropertyMeta<Bean, String> nameMeta =
                new PropertyMeta<>("name", String.class, Bean::getName, Bean::setName);

        @SuppressWarnings({ "rawtypes", "unchecked" })
        EntityMeta<Bean> em = new EntityMeta<>(
                (List) List.of(idMeta, activeMeta, statusMeta, nameMeta),
                List.of("id", "active", "status", "name"),
                List.of(Integer.class, Boolean.class, Status.class, String.class)
        );

        return new DefaultMetaRegistry().register("T", em);
    }

    @Test
    void bindsStringToIntegerUsingPropertyType() {
        QueryBinder qb = new QueryBinder(registry());

        QuerySpec spec = new QuerySpec("T", new EqSpec("id", "42"), null, null);
        Query<?> q = qb.fromSpec(spec);

        assertTrue(q.where() instanceof Eq);

        Bean b = new Bean();
        b.setId(42);

        @SuppressWarnings("unchecked")
        boolean ok = ConditionEvaluator.test(((Query<Bean>) q).where(), b);

        assertTrue(ok);
    }

    @Test
    void supportsAndOrNotBetweenCmpInIsNull() {
        QueryBinder qb = new QueryBinder(registry());

        CondSpec where = new AndSpec(List.of(
                new EqSpec("active", "true"),
                new NotSpec(new InSpec("id", List.of("1", "2", "3"))),
                new OrSpec(List.of(
                        new BetweenSpec("id", "10", "20"),
                        new CmpSpec("id", "GT", "100")
                )),
                new IsNotNullSpec("status")
        ));

        QuerySpec spec = new QuerySpec("T", where, null, 50);
        Query<Bean> q = qb.fromSpec(spec);

        assertNotNull(q.where());
        assertEquals(50, q.limit());

        Bean b = new Bean();
        b.setActive(true);
        b.setId(15);
        b.setStatus(Status.A);

        assertTrue(ConditionEvaluator.test(q.where(), b));

        b.setId(2); // fails NOT IN(1,2,3)
        assertFalse(ConditionEvaluator.test(q.where(), b));
    }

    @Test
    void coercesEnumViaOfNullableAndByNullable() {
        QueryBinder qb = new QueryBinder(registry());

        // status is enum; input is string, binder should find byCodeNullable(...)
        QuerySpec spec = new QuerySpec("T", new EqSpec("status", "2"), null, null);
        Query<Bean> q = qb.fromSpec(spec);

        assertTrue(q.where() instanceof Eq);

        Bean b = new Bean();
        b.setStatus(Status.B);

        assertTrue(ConditionEvaluator.test(q.where(), b));
    }

    @Test
    void bindsScanRangeAndLimit() {
        QueryBinder qb = new QueryBinder(registry());

        ScanSpec scan = new ScanSpec("id",
                new RangeSpec("10", "INCLUSIVE", "20", "EXCLUSIVE"));

        QuerySpec spec = new QuerySpec("T", new TrueSpec(), scan, 10);
        Query<Bean> q = qb.fromSpec(spec);

        assertNotNull(q.scan());
        assertEquals("id", q.scan().prop().getPropertyName());
        assertEquals(10, q.limit());

        assertEquals(10, q.scan().range().lower());
        assertEquals(Bound.INCLUSIVE, q.scan().range().lowerBound());
        assertEquals(20, q.scan().range().upper());
        assertEquals(Bound.EXCLUSIVE, q.scan().range().upperBound());
    }

    @Test
    void bindsLikeSpec() {
        QueryBinder qb = new QueryBinder(registry());

        QuerySpec spec = new QuerySpec("T", new LikeSpec("name", "Al%"), null, null);
        Query<Bean> q = qb.fromSpec(spec);

        Bean b = new Bean();
        b.setName("Alice");

        assertTrue(ConditionEvaluator.test(q.where(), b));
    }
}
