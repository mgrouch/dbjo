package org.github.dbjo.meta.db;

import java.util.List;
import java.util.Set;

public record TableModel(
        TableRef table,
        List<Col> cols,
        Set<String> pkColsUpper,
        List<IndexModel> indexes
) {}
