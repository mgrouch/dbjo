package org.github.dbjo.codegen.rdb;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.IndexModel;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.codegen.types.TypeMappings;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public final class RocksSchemaGenerator {
    private final Config cfg;

    public RocksSchemaGenerator(Config cfg) {
        this.cfg = cfg;
    }

    public int generateAll(List<TableModel> tables) throws IOException {
        Path outDir = cfg.codegenOutJava().resolve(cfg.schemaPkg().replace('.', '/'));
        Files.createDirectories(outDir);

        int n = 0;
        for (TableModel tm : tables) {
            String beanClass = Naming.toClassName(tm.table().table());
            String schemaClass = beanClass + cfg.schemaClassSuffix();

            String src = renderSchema(tm, beanClass, schemaClass);

            Path outFile = outDir.resolve(schemaClass + ".java");
            FilesUtil.writeString(outFile, src, cfg.overwrite());
            System.out.println("Wrote: " + outFile);
            n++;
        }
        return n;
    }

    private String renderSchema(TableModel tm, String beanClass, String schemaClass) {
        // CF naming: match your example ("users")
        String cfName = Naming.toLowerSnake(tm.table().table());
        String cfConst = Naming.toUpperConst(cfName) + cfg.cfConstSuffix(); // USERS_CF

        // mapper + proto FQNs
        String mapperClass = beanClass + cfg.protoMapperSuffix();
        String mapperFqn = cfg.protoMapperPkg() + "." + mapperClass;

        // Proto FQN (avoid import to prevent clashes with entity class)
        String protoFqn = cfg.protoJavaPkg() + "." + beanClass;

        // index models from DB
        List<IndexModel> indexes = tm.indexes() == null ? List.of() : tm.indexes();

        // column lookup for getter + type decisions
        Map<String, Col> colByUpper = new HashMap<>();
        for (Col c : tm.cols()) {
            if (c.colName() != null) colByUpper.put(c.colName().toUpperCase(Locale.ROOT), c);
        }

        // Decide if we need helper idxKey(...) (composite or non-String indexed types)
        boolean needIdxKey = false;
        boolean needBase64 = false;

        record GenIndex(
                IndexModel dbIdx,
                String constName,
                String constValue,
                List<String> getters,     // like "getEmail"
                boolean allStringSingles  // true if single-col and Java type is String
        ) {}

        List<GenIndex> genIdx = new ArrayList<>();
        for (IndexModel ix : indexes) {
            if (ix.indexName() == null || ix.indexName().isBlank()) continue;
            if (ix.columnNames() == null || ix.columnNames().isEmpty()) continue;

            // ✅ Skip pure PK indexes (Rocks primary key already provides this)
            if (isPkIndex(ix, tm.pkColsUpper())) continue;

            List<String> getters = new ArrayList<>();
            boolean allStringSingle = false;

            if (ix.columnNames().size() == 1) {
                Col c = colByUpper.get(ix.columnNames().get(0).toUpperCase(Locale.ROOT));
                if (c != null) {
                    var jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), null);
                    allStringSingle = "String".equals(jt.javaType());
                    if ("byte[]".equals(jt.javaType())) needBase64 = true;
                    if (!allStringSingle) needIdxKey = true;
                } else {
                    needIdxKey = true;
                }
            } else {
                needIdxKey = true;
                for (String cn : ix.columnNames()) {
                    Col c = colByUpper.get(cn.toUpperCase(Locale.ROOT));
                    if (c != null) {
                        var jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), null);
                        if ("byte[]".equals(jt.javaType())) needBase64 = true;
                    }
                }
            }

            for (String cn : ix.columnNames()) {
                String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(cn));
                getters.add("get" + Naming.capitalize(prop));
            }

            String idxConstName = makeIndexConstName(ix.indexName(), cfName);
            String idxConstValue = ix.indexName(); // keep DB index name as Rocks index name

            genIdx.add(new GenIndex(ix, idxConstName, idxConstValue, getters, allStringSingle));
        }

        // Key type + KeyCodec expr
        String keyType = inferKeyType(tm);
        String keyCodecExpr = keyCodecExprFor(keyType);

        StringBuilder sb = new StringBuilder(9000);
        sb.append("package ").append(cfg.schemaPkg()).append(";\n\n");

        sb.append("import org.github.dbjo.rdb.*;\n");

        // entity class import (if different pkg)
        if (!cfg.beanPkg().equals(cfg.schemaPkg())) {
            sb.append("import ").append(cfg.beanPkg()).append(".").append(beanClass).append(";\n");
        }

        // mapper import (avoid importing proto)
        if (!cfg.protoMapperPkg().equals(cfg.schemaPkg())) {
            sb.append("import ").append(mapperFqn).append(";\n");
        }

        sb.append("import org.rocksdb.ColumnFamilyHandle;\n");
        sb.append("import java.util.List;\n");
        if (needBase64) sb.append("import java.util.Base64;\n");
        sb.append("\n");

        sb.append("public final class ").append(schemaClass).append(" {\n");
        sb.append("    public static final String ").append(cfConst).append("  = \"").append(cfName).append("\";\n");

        for (GenIndex g : genIdx) {
            sb.append("    public static final String ").append(g.constName)
                    .append(" = \"").append(g.constValue).append("\";\n");
        }
        sb.append("\n");

        // def(...)
        String cfParam = Naming.toLowerCamel(cfName) + "Cf"; // usersCf
        sb.append("    public static EntityDef<").append(beanClass).append(", ").append(keyType)
                .append("> def(ColumnFamilyHandle ").append(cfParam).append(") {\n");

        sb.append("        Codec<").append(beanClass).append("> codec =\n");
        sb.append("                ProtobufPojoCodec.of(\n");
        sb.append("                        ").append(protoFqn).append(".getDefaultInstance(),\n");
        sb.append("                        new ").append(mapperClass).append("()\n");
        sb.append("                );\n\n");

        sb.append("        return EntityDef.of(\n");
        sb.append("                ").append(cfConst).append(",\n");
        sb.append("                ").append(cfParam).append(",\n");
        sb.append("                ").append(keyCodecExpr).append(",\n");
        sb.append("                codec,\n");
        sb.append("                List.of(\n");

        // indexes list
        if (genIdx.isEmpty()) {
            sb.append("                        // no DB indexes\n");
        } else {
            for (int i = 0; i < genIdx.size(); i++) {
                GenIndex g = genIdx.get(i);

                String factory = "unique";

                String extractor;
                if (g.allStringSingles && g.getters.size() == 1) {
                    extractor = beanClass + "::" + g.getters.get(0);
                } else {
                    String args = g.getters.stream()
                            .map(get -> "u." + get + "()")
                            .collect(Collectors.joining(", "));
                    extractor = "u -> idxKey(" + args + ")";
                }

                // ✅ Force <T,V> to stop inference failures in List.of(...)
                sb.append("                        IndexDef.<")
                        .append(beanClass)
                        .append(", String>")
                        .append(factory)
                        .append("(").append(g.constName)
                        .append(", IndexKeyCodec.stringUtf8(), ")
                        .append(extractor)
                        .append(")");

                sb.append(i < genIdx.size() - 1 ? ",\n" : "\n");
            }
        }

        sb.append("                )\n");
        sb.append("        );\n");
        sb.append("    }\n\n");

        // idxKey helper (only if needed)
        if (needIdxKey) {
            sb.append("    /**\n");
            sb.append("     * String index key builder. Returns null if any component is null (so index can skip nulls).\n");
            sb.append("     * Composite keys are joined with '\\u0000'.\n");
            sb.append("     */\n");
            sb.append("    private static String idxKey(Object... parts) {\n");
            sb.append("        if (parts == null || parts.length == 0) return null;\n");
            sb.append("        for (Object p : parts) if (p == null) return null;\n");
            sb.append("        if (parts.length == 1) return idxPart(parts[0]);\n");
            sb.append("        StringBuilder sb = new StringBuilder();\n");
            sb.append("        for (int i = 0; i < parts.length; i++) {\n");
            sb.append("            if (i > 0) sb.append('\\u0000');\n");
            sb.append("            sb.append(idxPart(parts[i]));\n");
            sb.append("        }\n");
            sb.append("        return sb.toString();\n");
            sb.append("    }\n\n");

            sb.append("    private static String idxPart(Object v) {\n");
            sb.append("        if (v == null) return null;\n");
            if (needBase64) {
                sb.append("        if (v instanceof byte[] b) return Base64.getEncoder().encodeToString(b);\n");
            } else {
                sb.append("        if (v instanceof byte[]) return \"\"; // unreachable unless you add byte[] indexes\n");
            }
            sb.append("        if (v instanceof java.math.BigDecimal bd) return bd.toPlainString();\n");
            sb.append("        return v.toString();\n");
            sb.append("    }\n\n");
        }

        sb.append("    private ").append(schemaClass).append("() {}\n");
        sb.append("}\n");

        return sb.toString();
    }

    private String inferKeyType(TableModel tm) {
        if (tm.pkColsUpper() == null || tm.pkColsUpper().isEmpty()) return "String";
        if (tm.pkColsUpper().size() != 1) return "String"; // composite PK fallback

        String pkUpper = tm.pkColsUpper().iterator().next();
        Col pkCol = null;
        for (Col c : tm.cols()) {
            if (c.colName() != null && c.colName().toUpperCase(Locale.ROOT).equals(pkUpper)) {
                pkCol = c;
                break;
            }
        }
        if (pkCol == null) return "String";
        return TypeMappings.mapSqlTypeToJava(pkCol.sqlType(), null).javaType();
    }

    private String keyCodecExprFor(String keyType) {
        return switch (keyType) {
            case "String" -> "KeyCodec.stringUtf8()";
            case "Integer", "Short" -> "KeyCodec.int32()";
            case "Long" -> "KeyCodec.int64()";
            case "byte[]" -> "KeyCodec.bytes()";
            case "UUID" -> "KeyCodec.uuid()";
            default -> "KeyCodec.stringUtf8()";
        };
    }

    private static boolean isPkIndex(IndexModel ix, Set<String> pkColsUpper) {
        if (pkColsUpper == null || pkColsUpper.isEmpty()) return false;
        if (ix.columnNames() == null || ix.columnNames().isEmpty()) return false;

        Set<String> idxCols = ix.columnNames().stream()
                .filter(Objects::nonNull)
                .map(s -> s.toUpperCase(Locale.ROOT))
                .collect(Collectors.toSet());

        return idxCols.equals(pkColsUpper);
    }

    private static String makeIndexConstName(String dbIndexName, String cfName) {
        String n = dbIndexName.toLowerCase(Locale.ROOT);
        String cf = cfName.toLowerCase(Locale.ROOT);
        if (n.startsWith(cf + "_") && n.endsWith("_idx") && n.length() > (cf.length() + 1 + 4)) {
            String mid = n.substring(cf.length() + 1, n.length() - 4);
            return "IDX_" + Naming.toUpperConst(mid);
        }
        return "IDX_" + Naming.toUpperConst(dbIndexName);
    }
}
