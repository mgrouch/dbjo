package org.github.dbjo.criteria.cache;

import java.io.Serializable;

public record QueryCacheKey(
        String canonicalString,
        String sha256Hex
) implements Serializable {}