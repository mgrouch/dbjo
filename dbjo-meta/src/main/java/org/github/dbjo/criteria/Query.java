package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Objects;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;

public final class Query<B extends Serializable> {

    private final EntityMeta<B> meta;
    private final Condition<B> where;
    private final Scan<B, ? extends Serializable> scan; // nullable
    private final Integer limit; // nullable

    private Query(EntityMeta<B> meta, Condition<B> where, Scan<B, ?> scan, Integer limit) {
        this.meta = Objects.requireNonNull(meta, "meta");
        this.where = Objects.requireNonNull(where, "where");
        this.scan = scan;
        this.limit = limit;
    }

    public EntityMeta<B> meta() { return meta; }
    public Condition<B> where() { return where; }
    public Scan<B, ? extends Serializable> scan() { return scan; }
    public Integer limit() { return limit; }

    public static <B extends Serializable> Builder<B> from(EntityMeta<B> meta) {
        return new Builder<>(meta);
    }

    public static final class Builder<B extends Serializable> {
        private final EntityMeta<B> meta;
        private Condition<B> where = Conditions.trueCondition();
        private Scan<B, ?> scan;
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

        public Builder<B> limit(int n) {
            if (n <= 0) throw new IllegalArgumentException("limit must be > 0");
            this.limit = n;
            return this;
        }

        public Query<B> build() { return new Query<>(meta, where, scan, limit); }
    }
}
