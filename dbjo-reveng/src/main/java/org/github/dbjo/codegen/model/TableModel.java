package org.github.dbjo.codegen.model;

import java.util.List;
import java.util.Set;

public record TableModel(TableRef table, List<Col> cols, Set<String> pkColsUpper) {}
