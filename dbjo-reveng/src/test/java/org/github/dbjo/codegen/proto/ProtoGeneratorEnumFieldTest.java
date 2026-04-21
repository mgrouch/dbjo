package org.github.dbjo.codegen.proto;

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

class ProtoGeneratorEnumFieldTest {

    @TempDir
    Path tmp;

    @Test
    void emitsNumericProtoFieldForEnumSuffixedColumn() throws Exception {
        Config cfg = Config.from(ArgMap.parse(new String[] {
                "--run=proto",
                "--outBase=" + tmp,
                "--protoOutProto=" + tmp,
                "--codegenOutJava=" + tmp
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
        new ProtoGenerator(cfg, enumIndex).generateAll(List.of(statusEnum, orders));

        Path protoPath = tmp.resolve("dbo_orders.proto");
        String src = Files.readString(protoPath);

        assertTrue(src.contains("int32 status_enum = 1;"));
    }
}
