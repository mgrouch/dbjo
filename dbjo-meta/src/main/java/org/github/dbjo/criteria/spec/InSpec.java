package org.github.dbjo.criteria.spec;

import java.util.List;

public record InSpec(String property, List<Object> values) implements CondSpec {}
