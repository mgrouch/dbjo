package org.github.dbjo.rdb;

import java.util.*;

public record Query<K>(KeyRange<K> range, int limit, boolean descending) {
    public Query {
        if (limit <= 0) limit = Integer.MAX_VALUE;
    }
}