package org.github.dbjo.criteria.spec;

public record CmpSpec(String property, String op, Object value) implements CondSpec {}
