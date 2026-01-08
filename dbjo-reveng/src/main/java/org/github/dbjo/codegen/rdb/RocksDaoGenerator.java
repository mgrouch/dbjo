package org.github.dbjo.codegen.rdb;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.model.Col;
import org.github.dbjo.codegen.model.TableModel;
import org.github.dbjo.codegen.types.TypeMappings;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DatabaseMetaData;
import java.util.*;

public final class RocksDaoGenerator {
    private final Config cfg;

    public RocksDaoGenerator(Config cfg) {
        this.cfg = cfg;
    }

    public int generateAll(List<TableModel> tables) throws IOException {
        Path daoDir = cfg.codegenOutJava().resolve(cfg.daoPkg().replace('.', '/'));
        Files.createDirectories(daoDir);

        int count = 0;
        for (TableModel tm : tables) {
            String beanClass = Naming.toClassName(tm.table().table());
            String daoClass = beanClass + cfg.daoClassSuffix();
            String schemaClass = beanClass + cfg.schemaClassSuffix();

            String cfConst = Naming.toUpperSnake(Naming.toFieldName(tm.table().table())) + cfg.cfConstSuffix();

            String keyType = inferKeyType(tm);

            String src = renderDao(cfg.daoPkg(), cfg.beanPkg(), cfg.schemaPkg(),
                    daoClass, beanClass, keyType, schemaClass, cfConst);

            Path outFile = daoDir.resolve(daoClass + ".java");
            FilesUtil.writeString(outFile, src, cfg.overwrite());
            System.out.println("Wrote: " + outFile);
            count++;
        }

        return count;
    }

    private String inferKeyType(TableModel tm) {
        if (tm.pkColsUpper().isEmpty()) return "String"; // fallback
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

        var jt = TypeMappings.mapSqlTypeToJava(pkCol.sqlType(), null);
        return jt.javaType();
    }

    private static String renderDao(
            String daoPkg,
            String beanPkg,
            String schemaPkg,
            String daoClass,
            String beanClass,
            String keyType,
            String schemaClass,
            String cfConst
    ) {
        boolean importBean = beanPkg != null && !beanPkg.equals(daoPkg);
        boolean importSchema = schemaPkg != null && !schemaPkg.equals(daoPkg);

        StringBuilder sb = new StringBuilder(2000);
        sb.append("package ").append(daoPkg).append(";\n\n");
        sb.append("import org.github.dbjo.rdb.*;\n");
        if (importBean) sb.append("import ").append(beanPkg).append(".").append(beanClass).append(";\n");
        if (importSchema) sb.append("import ").append(schemaPkg).append(".").append(schemaClass).append(";\n");
        sb.append("\n");

        sb.append("public final class ").append(daoClass)
                .append(" extends ").append("IndexedRocksDao")
                .append("<").append(beanClass).append(", ").append(keyType).append("> {\n");

        sb.append("    public ").append(daoClass).append("(RocksSessions sessions, DaoRegistry registry) {\n");
        sb.append("        super(sessions, registry.entity(\n");
        sb.append("                ").append(schemaClass).append(".def(registry.cf(").append(schemaClass).append(".").append(cfConst).append("))\n");
        sb.append("        ));\n");
        sb.append("    }\n");
        sb.append("}\n");

        return sb.toString();
    }
}
