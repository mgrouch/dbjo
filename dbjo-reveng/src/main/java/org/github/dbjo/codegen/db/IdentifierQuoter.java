package org.github.dbjo.codegen.db;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Helper for safely quoting SQL identifiers (schema/table/column) in generated SQL.
 *
 * Intended goals:
 *  - keep generated SQL valid when names collide with keywords or contain special chars
 *  - avoid quoting unless needed when mode=AUTO
 *
 * This quoter is ANSI-oriented by default (double quotes), but can incorporate
 * vendor keyword lists and case-folding behavior from {@link DatabaseMetaData}.
 */
public final class IdentifierQuoter {

    private static final Pattern SIMPLE_IDENT = Pattern.compile("[A-Za-z_]\\w*");

    private static final Set<String> EXTRA_KEYWORDS = Set.of(
            "USER", "ORDER", "GROUP", "SELECT", "FROM", "WHERE",
            "INSERT", "UPDATE", "DELETE", "MERGE", "INTO", "VALUES",
            "PRIMARY", "KEY", "UNIQUE", "INDEX",
            "TABLE", "SCHEMA", "DATABASE",
            "TIMESTAMP", "DATE", "TIME",
            "LEVEL", "VALUE", "DEFAULT"
    );

    private final SqlQuoteMode mode;
    private final String qOpen;
    private final String qClose;

    private final Set<String> keywordsUpper;

    // case-fold hints (best-effort)
    private final boolean storesUpper;
    private final boolean storesLower;
    private final boolean supportsMixed;

    private IdentifierQuoter(SqlQuoteMode mode,
                             String qOpen,
                             String qClose,
                             Set<String> keywordsUpper,
                             boolean storesUpper,
                             boolean storesLower,
                             boolean supportsMixed) {
        this.mode = Objects.requireNonNull(mode, "mode");
        this.qOpen = Objects.requireNonNull(qOpen, "qOpen");
        this.qClose = Objects.requireNonNull(qClose, "qClose");
        this.keywordsUpper = Objects.requireNonNull(keywordsUpper, "keywordsUpper");
        this.storesUpper = storesUpper;
        this.storesLower = storesLower;
        this.supportsMixed = supportsMixed;
    }

    /**
     * Builds a quoter that is dialect-neutral (ANSI quoting with double quotes),
     * while still using vendor metadata for keyword detection and identifier case behavior.
     */
    public static IdentifierQuoter ansiFromMeta(DatabaseMetaData md, SqlQuoteMode mode) throws SQLException {
        Objects.requireNonNull(md, "md");
        SqlQuoteMode m = (mode == null) ? SqlQuoteMode.AUTO : mode;

        Set<String> kws = new HashSet<>();

        String raw = md.getSQLKeywords();
        if (raw != null && !raw.isBlank()) {
            for (String k : raw.split(",")) {
                String kk = k.trim();
                if (!kk.isEmpty()) kws.add(kk.toUpperCase(Locale.ROOT));
            }
        }
        kws.addAll(EXTRA_KEYWORDS);

        boolean su = safeBool(md::storesUpperCaseIdentifiers);
        boolean sl = safeBool(md::storesLowerCaseIdentifiers);
        boolean sm = safeBool(md::supportsMixedCaseIdentifiers);

        return new IdentifierQuoter(m, "\"", "\"", kws, su, sl, sm);
    }

    /** Simple constructor for tests / manual wiring. Uses ANSI quotes. */
    public static IdentifierQuoter ansi(SqlQuoteMode mode,
                                        Set<String> keywordsUpper,
                                        boolean storesUpper,
                                        boolean storesLower,
                                        boolean supportsMixed) {
        Set<String> kws = new HashSet<>();
        if (keywordsUpper != null) {
            for (String k : keywordsUpper) {
                if (k != null && !k.isBlank()) kws.add(k.trim().toUpperCase(Locale.ROOT));
            }
        }
        kws.addAll(EXTRA_KEYWORDS);

        return new IdentifierQuoter(
                mode == null ? SqlQuoteMode.AUTO : mode,
                "\"", "\"",
                kws,
                storesUpper, storesLower, supportsMixed
        );
    }

    public String schemaTable(String schema, String table) {
        if (schema == null || schema.isBlank()) return id(table);
        return id(schema) + "." + id(table);
    }

    public String id(String ident) {
        if (ident == null) throw new IllegalArgumentException("identifier is null");

        if (isSurroundedByQuotes(ident)) return ident;

        if (mode == SqlQuoteMode.NONE) return ident;
        if (mode == SqlQuoteMode.ALWAYS) return quote(ident);

        // AUTO:
        if (!SIMPLE_IDENT.matcher(ident).matches()) return quote(ident);
        if (isKeyword(ident)) return quote(ident);

        // Case-fold heuristics:
        //  - If DB stores UPPER identifiers, a non-UPPER name likely requires quoting.
        //  - If DB stores lower identifiers, a non-lower name likely requires quoting.
        //  - If mixed-case identifiers are supported unquoted, do not force quoting for case alone.
        if (!supportsMixed) {
            if (storesUpper && !isAllUpper(ident)) return quote(ident);
            if (storesLower && !isAllLower(ident)) return quote(ident);
        }

        return ident;
    }

    private boolean isKeyword(String ident) {
        return keywordsUpper.contains(ident.toUpperCase(Locale.ROOT));
    }

    private String quote(String ident) {
        String esc = ident.replace(qClose, qClose + qClose);
        return qOpen + esc + qClose;
    }

    private static boolean isAllUpper(String s) {
        boolean hasLetter = false;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isLetter(ch)) {
                hasLetter = true;
                if (!Character.isUpperCase(ch)) return false;
            }
        }
        return hasLetter;
    }

    private static boolean isAllLower(String s) {
        boolean hasLetter = false;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isLetter(ch)) {
                hasLetter = true;
                if (!Character.isLowerCase(ch)) return false;
            }
        }
        return hasLetter;
    }

    private boolean isSurroundedByQuotes(String s) {
        return s.length() >= 2 && s.startsWith(qOpen) && s.endsWith(qClose);
    }

    private interface BoolSupplier { boolean get() throws SQLException; }

    private static boolean safeBool(BoolSupplier s) {
        try { return s.get(); } catch (Exception e) { return false; }
    }
}
