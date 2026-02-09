package org.github.dbjo.meta.validation;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.List;

public final class ValidationSupport {
    private ValidationSupport() {
    }

    public static void throwIfAny(List<String> errors) {
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join("; ", errors));
        }
    }

    public static boolean isValidYyyyMmDd(Number value) {
        if (value == null) {
            return false;
        }
        long raw = value.longValue();
        if (raw < 10_000_000L || raw > 99_99_12_31L) {
            return false;
        }

        int ymd = (int) raw;
        int day = ymd % 100;
        int month = (ymd / 100) % 100;
        int year = ymd / 10_000;

        try {
            LocalDate.of(year, month, day);
            return true;
        } catch (DateTimeException ex) {
            return false;
        }
    }
}
