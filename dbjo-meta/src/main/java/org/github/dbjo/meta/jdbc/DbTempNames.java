package org.github.dbjo.meta.jdbc;

import java.util.Locale;

public final class DbTempNames {
    private DbTempNames() {}

    /** Conservative: letters/digits/_ only; lowercased; never empty. */
    public static String safeSuffix(String suffix) {
        if (suffix == null) return "x";
        String s = suffix.trim().toLowerCase(Locale.ROOT);
        if (s.isEmpty()) return "x";

        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') out.append(c);
        }
        if (out.length() == 0) return "x";
        return out.toString();
    }

    public static String safeBase(String base) {
        if (base == null) return "tmp";
        String s = base.trim();
        if (s.isEmpty()) return "tmp";

        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                    (c >= '0' && c <= '9') || c == '_') out.append(c);
        }
        if (out.length() == 0) return "tmp";
        return out.toString();
    }

    /**
     * Dialect-aware temp name. Prefix (e.g. '#') is dialect-provided.
     * Ensures final name length respects dialect identifier max length.
     */
    public static String tempName(DbDialect dialect, String base, String suffix) {
        String pfx = dialect.tempPrefix() == null ? "" : dialect.tempPrefix();
        String b = safeBase(base);
        String sfx = safeSuffix(suffix);

        String core = b + "_" + sfx;
        int max = Math.max(8, dialect.identMaxLen());
        int maxCore = Math.max(1, max - pfx.length());

        if (core.length() > maxCore) {
            // keep suffix intact; trim base part
            int keep = Math.max(1, maxCore - (1 + sfx.length())); // "_" + sfx
            String b2 = b.length() > keep ? b.substring(0, keep) : b;
            core = b2 + "_" + sfx;
            if (core.length() > maxCore) core = core.substring(0, maxCore);
        }
        return pfx + core;
    }
}
