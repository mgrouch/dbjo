package org.github.dbjo.codegen.jdbc;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.codegen.types.TypeMappings;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * Generates one concrete JDBC DAO per table:
 *
 *   <Entity><jdbcDaoClassSuffix> extends BaseJdbcDAO<Entity, Key>
 */
public final class JdbcDaoGenerator {
    private final Config cfg;

    public JdbcDaoGenerator(Config cfg) {
        this.cfg = cfg;
    }

    public int generateAll(List<TableModel> tables) throws IOException {
        Path daoDir = cfg.codegenOutJava().resolve(cfg.jdbcDaoPkg().replace('.', '/'));
        Files.createDirectories(daoDir);

        int count = 0;
        for (TableModel tm : tables) {
            String beanClass = Naming.toClassName(tm.table().table());
            String daoClass = beanClass + cfg.jdbcDaoClassSuffix();
            String keyType = inferKeyType(tm);
            String metaClass = beanClass + "DbMeta";

            String src = renderDao(cfg.jdbcDaoPkg(), cfg.beanPkg(), cfg.dbMetaPkg(),
                    daoClass, beanClass, keyType, metaClass, cfg.jdbcDaoBaseClass());

            Path outFile = daoDir.resolve(daoClass + ".java");
            FilesUtil.writeString(outFile, src, cfg.overwrite());
            System.out.println("Wrote: " + outFile);
            count++;
        }

        return count;
    }

    private String inferKeyType(TableModel tm) {
        if (tm.pkColsUpper().isEmpty()) return "Void"; // no PK
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
            String dbMetaPkg,
            String daoClass,
            String beanClass,
            String keyType,
            String metaClass,
            String baseDaoClass
    ) {
        boolean importBean = beanPkg != null && !beanPkg.equals(daoPkg);
        boolean importMeta = dbMetaPkg != null && !dbMetaPkg.equals(daoPkg);

        String baseDaoFqn = baseDaoClass.contains(".") ? baseDaoClass : ("org.github.dbjo.meta.jdbc." + baseDaoClass);
        String baseDaoSimple = baseDaoClass.contains(".")
                ? baseDaoClass.substring(baseDaoClass.lastIndexOf('.') + 1)
                : baseDaoClass;

        StringBuilder sb = new StringBuilder(1500);
        sb.append("package ").append(daoPkg).append(";\n\n");
        sb.append("import javax.sql.DataSource;\n");
        sb.append("import org.github.dbjo.meta.jdbc.DbDialect;\n");
        sb.append("import ").append(baseDaoFqn).append(";\n");
        if (importBean) sb.append("import ").append(beanPkg).append(".").append(beanClass).append(";\n");
        if (importMeta) sb.append("import ").append(dbMetaPkg).append(".").append(metaClass).append(";\n");
        sb.append("\n");

        sb.append("public final class ").append(daoClass)
                .append(" extends ").append(baseDaoSimple)
                .append("<").append(beanClass).append(", ").append(keyType).append("> {\n");

        sb.append("    public ").append(daoClass).append("(DataSource ds, DbDialect dialect) {\n");
        sb.append("        super(ds, dialect, ").append(metaClass).append(".INSTANCE);\n");
        sb.append("    }\n");

        sb.append("}\n");
        return sb.toString();
    }
}
