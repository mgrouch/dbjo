package org.github.dbjo.criteria.spec;

public record BetweenSpec(String property, Object lo, Object hi) implements CondSpec {}