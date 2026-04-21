package org.github.dbjo.codegen.rdb;

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
import static org.junit.jupiter.api.Assertions.assertFalse;

class ProtoMapperGeneratorTest {

    @TempDir
    Path tmp;

    @Test
    void mapsNullableBigDecimalFromProtoEmptyStringToNull() throws Exception {
        Config cfg = Config.from(ArgMap.parse(new String[] {
                "--run=mapper",
                "--outBase=" + tmp,
                "--codegenOutJava=" + tmp,
                "--protoJavaPkg=org.github.dbjo.generated.proto",
                "--protoMapperPkg=org.github.dbjo.generated.rdb.mapper",
                "--beanPkg=org.github.dbjo.generated.model.entity"
        }));

        TableModel orders = new TableModel(
                new TableRef("dbo", "orders"),
                List.of(new Col(1, "amount", Types.DECIMAL, "DECIMAL", 20, 2, Nullability.NULLABLE, false, null)),
                Set.of(),
                List.of()
        );

        new ProtoMapperGenerator(cfg).generateAll(List.of(orders));
        Path mapperPath = tmp.resolve("org/github/dbjo/generated/rdb/mapper/OrdersProtoMapper.java");
        String src = Files.readString(mapperPath);

        assertTrue(src.contains("u.setAmount(p.getAmount().isEmpty() ? null : new BigDecimal(p.getAmount()));"));
    }

    @Test
    void mapsEnumSuffixedColumnsUsingEnumKeyNumericType() throws Exception {
        Config cfg = Config.from(ArgMap.parse(new String[] {
                "--run=mapper",
                "--outBase=" + tmp,
                "--codegenOutJava=" + tmp,
                "--protoJavaPkg=org.github.dbjo.generated.proto",
                "--protoMapperPkg=org.github.dbjo.generated.rdb.mapper",
                "--beanPkg=org.github.dbjo.generated.model.entity"
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
        new ProtoMapperGenerator(cfg, enumIndex).generateAll(List.of(statusEnum, orders));

        Path mapperPath = tmp.resolve("org/github/dbjo/generated/rdb/mapper/OrdersProtoMapper.java");
        String src = Files.readString(mapperPath);

        assertTrue(src.contains("if (pojo.getStatusEnum() != null) b.setStatusEnum(pojo.getStatusEnum());"));
        assertTrue(src.contains("u.setStatusEnum(p.getStatusEnum());"));
        assertFalse(src.contains("StatusEnum."));
    }
}
