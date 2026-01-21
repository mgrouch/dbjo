package org.github.dbjo.criteria.spec;

import java.util.List;

public record OrSpec(List<CondSpec> items) implements CondSpec {}
