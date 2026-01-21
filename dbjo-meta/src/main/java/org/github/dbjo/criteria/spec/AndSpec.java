package org.github.dbjo.criteria.spec;

import java.util.List;

public record AndSpec(List<CondSpec> items) implements CondSpec {}