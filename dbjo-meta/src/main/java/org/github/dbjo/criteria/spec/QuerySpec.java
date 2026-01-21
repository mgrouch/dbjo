package org.github.dbjo.criteria.spec;

import java.io.Serializable;

public record QuerySpec(
        String entityId,     // stable id (e.g., "PUBLIC.CLIENT" or "Client")
        CondSpec where,      // nullable => TRUE
        ScanSpec scan,       // nullable
        Integer limit        // nullable
) implements Serializable {}
