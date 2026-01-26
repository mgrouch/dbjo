package org.github.dbjo.codegen.db;

import java.util.Locale;

public enum SqlQuoteMode {
    NONE,
    AUTO,
    ALWAYS;

    public static SqlQuoteMode parse(String raw) {
        if (raw == null || raw.isBlank()) return AUTO;
        String s = raw.trim().toLowerCase(Locale.ROOT);
        return switch (s) {
            case "none", "off", "false", "0" -> NONE;
            case "auto", "on", "true", "1" -> AUTO;
            case "always", "force" -> ALWAYS;
            default -> AUTO;
        };
    }
}
