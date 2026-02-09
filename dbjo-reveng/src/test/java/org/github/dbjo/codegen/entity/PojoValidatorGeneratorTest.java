package org.github.dbjo.codegen.entity;

import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.Nullability;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.meta.db.TableRef;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PojoValidatorGeneratorTest {

    @Test
    void renderValidator_emitsNullabilityAndLengthAndNumericChecks() {
        TableModel tm = new TableModel(
                new TableRef("PUBLIC", "CLIENT"),
                List.of(
                        new Col(1, "ID", Types.INTEGER, "INTEGER", 10, 0, Nullability.NO_NULLS, false, null),
                        new Col(2, "NAME", Types.VARCHAR, "VARCHAR", 32, 0, Nullability.NO_NULLS, false, null),
                        new Col(3, "EMAIL", Types.VARCHAR, "VARCHAR", 128, 0, Nullability.NULLABLE, false, null),
                        new Col(4, "AMOUNT", Types.DECIMAL, "DECIMAL", 10, 2, Nullability.NULLABLE, false, null)
                ),
                Set.of("ID"),
                List.of()
        );

        String src = PojoValidatorGenerator.renderValidator(
                "org.example.validator",
                "org.example.bean",
                "Client",
                "ClientValidator",
                tm
        );

        assertTrue(src.contains("if (pojo.getId() == null) errors.add(\"ID must not be null\")"));
        assertTrue(src.contains("if (pojo.getName() == null) errors.add(\"NAME must not be null\")"));
        assertTrue(src.contains("if (pojo.getName() != null && pojo.getName().length() > 32) errors.add(\"NAME length must be <= 32\")"));
        assertTrue(src.contains("if (pojo.getEmail() != null && pojo.getEmail().length() > 128) errors.add(\"EMAIL length must be <= 128\")"));
        assertTrue(src.contains("if (pojo.getAmount() != null && pojo.getAmount().scale() > 2) errors.add(\"AMOUNT scale must be <= 2\")"));
        assertTrue(src.contains("if (pojo.getAmount() != null && pojo.getAmount().precision() > 10) errors.add(\"AMOUNT precision must be <= 10\")"));
    }
}
