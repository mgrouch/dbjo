package org.github.dbjo.codegen.model;

import java.util.List;

public record IndexModel(
        String indexName,          // as in DB
        boolean unique,            // derived from NON_UNIQUE
        List<String> columnNames   // in ORDINAL_POSITION order
) {}
