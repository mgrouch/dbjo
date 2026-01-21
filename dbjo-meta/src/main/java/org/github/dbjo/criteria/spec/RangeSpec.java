package org.github.dbjo.criteria.spec;

import java.io.Serializable;

public record RangeSpec(
        Object lower, String lowerBound,   // "INCLUSIVE" | "EXCLUSIVE" | "UNBOUNDED"
        Object upper, String upperBound
) implements Serializable {}
