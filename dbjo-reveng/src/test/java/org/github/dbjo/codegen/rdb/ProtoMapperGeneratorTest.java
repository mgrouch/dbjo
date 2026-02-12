package org.github.dbjo.codegen.rdb;

import org.github.dbjo.codegen.ArgMap;
import org.github.dbjo.codegen.Config;
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
}
