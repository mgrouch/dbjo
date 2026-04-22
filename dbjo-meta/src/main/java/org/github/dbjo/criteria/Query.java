package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;

public final class Query<B extends Serializable> {

    private final EntityMeta<B> meta;
    private final Condition<B> where;
    private final Scan<B, ? extends Serializable> scan; // nullable
    private final List<Order<B, ? extends Serializable>> orderBy;
    private final Integer limit; // nullable

    private Query(EntityMeta<B> meta,
                  Condition<B> where,
                  Scan<B, ?> scan,
                  List<Order<B, ? extends Serializable>> orderBy,
                  Integer limit) {
        this.meta = Objects.requireNonNull(meta, "meta");
        this.where = Objects.requireNonNull(where, "where");
        this.scan = scan;
        this.orderBy = List.copyOf(orderBy == null ? List.of() : orderBy);
        this.limit = limit;
    }

    public EntityMeta<B> meta() { return meta; }
    public Condition<B> where() { return where; }
    public Scan<B, ? extends Serializable> scan() { return scan; }
    public List<Order<B, ? extends Serializable>> orderBy() { return orderBy; }
    public Integer limit() { return limit; }

    public static <B extends Serializable> Builder<B> from(EntityMeta<B> meta) {
        return new Builder<>(meta);
    }

    public static final class Builder<B extends Serializable> {
        private final EntityMeta<B> meta;
        private Condition<B> where = Conditions.trueCondition();
        private Scan<B, ?> scan;
        private final List<Order<B, ? extends Serializable>> orderBy = new ArrayList<>();
        private Integer limit;

        private Builder(EntityMeta<B> meta) { this.meta = Objects.requireNonNull(meta, "meta"); }

        public Builder<B> where(Condition<B> where) {
            this.where = (where == null) ? Conditions.trueCondition() : where;
            return this;
        }

        public <V extends Serializable> Builder<B> scan(PropertyMeta<B, V> prop, Range<V> range) {
            this.scan = new Scan<>(prop, range);
            return this;
        }

        public <V extends Serializable> Builder<B> orderByAsc(PropertyMeta<B, V> prop) {
            return orderBy(prop, OrderDirection.ASC);
        }

        public <V extends Serializable> Builder<B> orderByDesc(PropertyMeta<B, V> prop) {
            return orderBy(prop, OrderDirection.DESC);
        }

        public <V extends Serializable> Builder<B> orderBy(PropertyMeta<B, V> prop, OrderDirection dir) {
            this.orderBy.add(new Order<>(Objects.requireNonNull(prop, "prop"), Objects.requireNonNull(dir, "dir")));
            return this;
        }

        public Builder<B> limit(int n) {
            if (n <= 0) throw new IllegalArgumentException("limit must be > 0");
            this.limit = n;
            return this;
        }

        public Query<B> build() { return new Query<>(meta, where, scan, orderBy, limit); }
    }

    public enum OrderDirection {
        ASC, DESC
    }

    public record Order<B extends Serializable, V extends Serializable>(PropertyMeta<B, V> prop, OrderDirection dir) {
        public Order {
            Objects.requireNonNull(prop, "prop");
            Objects.requireNonNull(dir, "dir");
        }
    }
}
