package org.github.dbjo.rdb;

import java.util.*;

public record Query<K>(
        Optional<KeyRange<K>> keyRange,
        List<IndexPredicate> indexPredicates,
        int limit,
        boolean descending
) {
    public Query {
        if (limit <= 0) limit = Integer.MAX_VALUE;
        indexPredicates = (indexPredicates == null) ? List.of() : List.copyOf(indexPredicates);
        keyRange = (keyRange.isEmpty()) ? Optional.empty() : keyRange;

        // Current DAO planning only supports a single index predicate.
        if (indexPredicates.size() > 1) {
            throw new IllegalArgumentException(
                    "Only one index predicate is supported (got " + indexPredicates.size() + ")"
            );
        }
    }

    public static <K> Builder<K> builder() { return new Builder<>(); }

    public static final class Builder<K> {
        private KeyRange<K> range;
        private final List<IndexPredicate> preds = new ArrayList<>();
        private int limit = Integer.MAX_VALUE;
        private boolean desc = false;

        public Builder<K> range(KeyRange<K> r) { this.range = r; return this; }
        public Builder<K> where(IndexPredicate p) { this.preds.add(p); return this; }
        public Builder<K> limit(int n) { this.limit = n; return this; }
        public Builder<K> descending(boolean d) { this.desc = d; return this; }
        public Query<K> build() { return new Query<>(Optional.ofNullable(range), preds, limit, desc); }
    }
}
