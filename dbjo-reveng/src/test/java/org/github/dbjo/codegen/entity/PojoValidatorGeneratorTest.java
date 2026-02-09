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
                        new Col(4, "AMOUNT", Types.DECIMAL, "DECIMAL", 10, 2, Nullability.NULLABLE, false, null),
                        new Col(5, "BIRTH_DATE", Types.INTEGER, "INTEGER", 10, 0, Nullability.NULLABLE, false, null)
                ),
                Set.of("ID"),
                List.of()
        );

        String src = PojoValidatorGenerator.renderValidator(
                "org.example.validator",
                "org.example.bean",
                "org.example.dbschema",
                "Client",
                "ClientValidator",
                tm
        );

        assertTrue(src.contains("private static final Map<String, Col> COLS_BY_NAME = ValidationSupport.colsByName("));
        assertTrue(src.contains("ValidationSupport.validateNullableAndLength(errors, \"ID\", COLS_BY_NAME.get(\"ID\"), pojo.getId());"));
        assertTrue(src.contains("ValidationSupport.validateNullableAndLength(errors, \"NAME\", COLS_BY_NAME.get(\"NAME\"), pojo.getName());"));
        assertTrue(src.contains("ValidationSupport.validateNullableAndLength(errors, \"EMAIL\", COLS_BY_NAME.get(\"EMAIL\"), pojo.getEmail());"));
        assertTrue(src.contains("if (pojo.getAmount() != null && pojo.getAmount().scale() > 2) errors.add(\"AMOUNT scale must be <= 2\")"));
        assertTrue(src.contains("if (pojo.getAmount() != null && pojo.getAmount().precision() > 10) errors.add(\"AMOUNT precision must be <= 10\")"));
        assertTrue(src.contains("if (pojo.getBirthDate() != null && !ValidationSupport.isValidYyyyMmDd(pojo.getBirthDate())) errors.add(\"BIRTH_DATE must be a valid date in yyyyMMdd format\")"));
        assertTrue(src.contains("ValidationSupport.throwIfAny(validate(pojo));"));
    }

    @Test
    void renderValidator_usesBoxedVersionGetter() {
        TableModel tm = new TableModel(
                new TableRef("PUBLIC", "CLIENT"),
                List.of(new Col(1, "VERSION", Types.INTEGER, "INTEGER", 10, 0, Nullability.NULLABLE, false, null)),
                Set.of(),
                List.of()
        );

        String src = PojoValidatorGenerator.renderValidator(
                "org.example.validator",
                "org.example.bean",
                "org.example.dbschema",
                "Client",
                "ClientValidator",
                tm
        );

        assertTrue(src.contains("ValidationSupport.validateNullableAndLength(errors, \"VERSION\", COLS_BY_NAME.get(\"VERSION\"), pojo.getVersionBoxed());"));
    }
}
