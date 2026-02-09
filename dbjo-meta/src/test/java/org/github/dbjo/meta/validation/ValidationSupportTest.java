package org.github.dbjo.meta.validation;

import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.Nullability;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.meta.db.TableRef;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidationSupportTest {

    @Test
    void isValidYyyyMmDd_handlesValidAndInvalidDates() {
        assertTrue(ValidationSupport.isValidYyyyMmDd(20240229));
        assertFalse(ValidationSupport.isValidYyyyMmDd(20230229));
        assertFalse(ValidationSupport.isValidYyyyMmDd(20241301));
        assertFalse(ValidationSupport.isValidYyyyMmDd(999999));
    }

    @Test
    void throwIfAny_throwsOnlyWhenErrorsPresent() {
        assertDoesNotThrow(() -> ValidationSupport.throwIfAny(List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> ValidationSupport.throwIfAny(List.of("one", "two")));
    }

    @Test
    void colsByName_andValidateNullableAndLength_useTableMetadata() {
        TableModel tm = new TableModel(
                new TableRef("PUBLIC", "CLIENT"),
                List.of(
                        new Col(1, "NAME", Types.VARCHAR, "VARCHAR", 4, 0, Nullability.NO_NULLS, false, null),
                        new Col(2, "PAYLOAD", Types.VARBINARY, "VARBINARY", 2, 0, Nullability.NULLABLE, false, null)
                ),
                Set.of(),
                List.of()
        );

        Map<String, Col> cols = ValidationSupport.colsByName(tm);
        List<String> errors = new ArrayList<>();

        ValidationSupport.validateNullableAndLength(errors, "NAME", cols.get("NAME"), null);
        ValidationSupport.validateNullableAndLength(errors, "NAME", cols.get("NAME"), "TOO_LONG");
        ValidationSupport.validateNullableAndLength(errors, "PAYLOAD", cols.get("PAYLOAD"), new byte[]{1, 2, 3});

        assertTrue(errors.contains("NAME must not be null"));
        assertTrue(errors.contains("NAME length must be <= 4"));
        assertTrue(errors.contains("PAYLOAD byte length must be <= 2"));
    }
}
