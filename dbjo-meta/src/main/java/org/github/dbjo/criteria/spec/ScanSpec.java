package org.github.dbjo.criteria.spec;

import java.io.Serializable;

public record ScanSpec(
        String property,     // propertyName
        RangeSpec range
) implements Serializable {}
