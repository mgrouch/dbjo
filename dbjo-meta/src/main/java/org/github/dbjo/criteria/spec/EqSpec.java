package org.github.dbjo.criteria.spec;

public record EqSpec(String property, Object value) implements CondSpec {}