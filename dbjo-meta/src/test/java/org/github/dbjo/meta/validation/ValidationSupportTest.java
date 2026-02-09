package org.github.dbjo.meta.validation;

import org.junit.jupiter.api.Test;

import java.util.List;

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
}
