package org.github.dbjo.criteria.bind;

import org.github.dbjo.criteria.*;
import org.github.dbjo.criteria.spec.*;
import org.github.dbjo.meta.entity.DefaultMetaRegistry;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Extra coverage for {@link QueryBinder} value coercion.
 */
final class QueryBinderCoercionTest {

    // minimal bean
    public static final class Foo implements Serializable {
        private Integer id;
        private Long longId;
        private Short small;
        private Boolean active;
        private Status status;
        private Plain plain;

        public Integer getId() { return id; }
        public void setId(Integer id) { this.id = id; }
        public Long getLongId() { return longId; }
        public void setLongId(Long longId) { this.longId = longId; }
        public Short getSmall() { return small; }
        public void setSmall(Short small) { this.small = small; }
        public Boolean getActive() { return active; }
        public void setActive(Boolean active) { this.active = active; }
        public Status getStatus() { return status; }
        public void setStatus(Status status) { this.status = status; }
        public Plain getPlain() { return plain; }
        public void setPlain(Plain plain) { this.plain = plain; }
    }

    public enum Plain {
        A, B
    }

    public enum Status {
        ACTIVE(1), DISABLED(2);

        private final int code;
        Status(int code) { this.code = code; }

        public int code() { return code; }

        public static Status of(int code) {
            for (Status s : values()) if (s.code == code) return s;
            throw new IllegalArgumentException("Unknown code: " + code);
        }

        public static Status ofNullable(Integer code) {
            if (code == null) return null;
            for (Status s : values()) if (s.code == code) return s;
            return null;
        }

        public static Status byCodeNullable(String code) {
            if (code == null) return null;
            try {
                return ofNullable(Integer.parseInt(code.trim()));
            } catch (Exception ignored) {
                return null;
            }
        }
    }

    private static final PropertyMeta<Foo, Integer> ID =
            new PropertyMeta<>("id", Integer.class, Foo::getId, Foo::setId);

    private static final PropertyMeta<Foo, Long> LONG_ID =
            new PropertyMeta<>("longId", Long.class, Foo::getLongId, Foo::setLongId);

    private static final PropertyMeta<Foo, Short> SMALL =
            new PropertyMeta<>("small", Short.class, Foo::getSmall, Foo::setSmall);

    private static final PropertyMeta<Foo, Boolean> ACTIVE =
            new PropertyMeta<>("active", Boolean.class, Foo::getActive, Foo::setActive);

    private static final PropertyMeta<Foo, Status> STATUS =
            new PropertyMeta<>("status", Status.class, Foo::getStatus, Foo::setStatus);

    private static final PropertyMeta<Foo, Plain> PLAIN =
            new PropertyMeta<>("plain", Plain.class, Foo::getPlain, Foo::setPlain);

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final EntityMeta<Foo> META = new EntityMeta(
            (List) List.of(ID, LONG_ID, SMALL, ACTIVE, STATUS, PLAIN),
            List.of("id", "longId", "small", "active", "status", "plain"),
            List.of(Integer.class, Long.class, Short.class, Boolean.class, Status.class, Plain.class)
    );

    private static QueryBinder binder() {
        return new QueryBinder(new DefaultMetaRegistry().register("Foo", META));
    }

    @Test
    void numericAndBooleanCoercions_work() {
        QuerySpec spec = new QuerySpec(
                "Foo",
                new AndSpec(List.of(
                        new EqSpec("id", " 42 "),
                        new EqSpec("longId", 7),
                        new EqSpec("small", "3"),
                        new EqSpec("active", "true")
                )),
                null,
                null
        );

        Query<Foo> q = binder().fromSpec(spec);
        assertTrue(q.where() instanceof And<Foo>);

        // Walk the tree to ensure types are coerced.
        And<Foo> and1 = (And<Foo>) q.where();
        assertTrue(and1.left() instanceof And<Foo>);
        And<Foo> and2 = (And<Foo>) and1.left();
        assertTrue(and2.left() instanceof And<Foo>);
        And<Foo> and3 = (And<Foo>) and2.left();

        Eq<Foo, ?> idEq = (Eq<Foo, ?>) and3.left();
        Eq<Foo, ?> longEq = (Eq<Foo, ?>) and3.right();
        Eq<Foo, ?> smallEq = (Eq<Foo, ?>) and2.right();
        Eq<Foo, ?> activeEq = (Eq<Foo, ?>) and1.right();

        assertEquals(42, idEq.value());
        assertEquals(7L, longEq.value());
        assertEquals((short) 3, smallEq.value());
        assertEquals(true, activeEq.value());
    }

    @Test
    void enumCoercion_prefers_ofNullable_then_byNullable_then_valueOf() {
        // status: "1" -> byCodeNullable(String) -> ACTIVE
        QuerySpec spec = new QuerySpec("Foo", new EqSpec("status", "1"), null, null);
        Query<Foo> q = binder().fromSpec(spec);

        assertTrue(q.where() instanceof Eq<Foo, ?>);
        Eq<Foo, ?> e = (Eq<Foo, ?>) q.where();
        assertEquals(Status.ACTIVE, e.value());

        // plain: "A" -> Enum.valueOf fallback
        QuerySpec spec2 = new QuerySpec("Foo", new EqSpec("plain", "A"), null, null);
        Query<Foo> q2 = binder().fromSpec(spec2);
        assertTrue(q2.where() instanceof Eq<Foo, ?>);
        Eq<Foo, ?> e2 = (Eq<Foo, ?>) q2.where();
        assertEquals(Plain.A, e2.value());
    }
}
