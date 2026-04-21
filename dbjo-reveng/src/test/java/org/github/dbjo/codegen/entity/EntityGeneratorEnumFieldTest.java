package org.github.dbjo.codegen.entity;

import org.github.dbjo.codegen.ArgMap;
import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.util.EnumIndex;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.Nullability;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.meta.db.TableRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityGeneratorEnumFieldTest {

    @TempDir
    Path tmp;

    @Test
    void usesEnumKeyNumericTypeForEnumSuffixedField() throws Exception {
        Config cfg = Config.from(ArgMap.parse(new String[] {
                "--run=entity",
                "--outBase=" + tmp,
                "--codegenOutJava=" + tmp,
                "--beanPkg=org.github.dbjo.generated.model.entity",
                "--metaPkg=org.github.dbjo.generated.model.meta"
        }));

        TableModel statusEnum = new TableModel(
                new TableRef("dbo", "status_enum"),
                List.of(new Col(1, "id", Types.INTEGER, "INTEGER", 10, 0, Nullability.NO_NULLS, false, null)),
                Set.of("ID"),
                List.of()
        );
        TableModel orders = new TableModel(
                new TableRef("dbo", "orders"),
                List.of(new Col(1, "status_enum", Types.VARCHAR, "VARCHAR", 32, 0, Nullability.NULLABLE, false, null)),
                Set.of(),
                List.of()
        );

        EnumIndex enumIndex = EnumIndex.fromTables(List.of(statusEnum, orders));
        new EntityGenerator(cfg, enumIndex).generateAll(List.of(statusEnum, orders));

        Path beanPath = tmp.resolve("org/github/dbjo/generated/model/entity/Orders.java");
        String src = Files.readString(beanPath);

        assertTrue(src.contains("private Integer statusEnum;"));
        assertTrue(src.contains("public Integer getStatusEnum()"));
        assertTrue(src.contains("public void setStatusEnum(Integer statusEnum)"));
    }
}
