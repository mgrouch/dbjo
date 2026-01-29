package org.github.dbjo.rdb.jdbc.catalog;

import org.github.dbjo.criteria.Condition;
import org.github.dbjo.criteria.Conditions;
import org.github.dbjo.criteria.PropertyTerm;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Compiles a SQL-ish WHERE clause string into dbjo Criteria {@link Condition}.
 *
 * Supported:
 *  - AND / OR / NOT with parentheses
 *  - IS NULL / IS NOT NULL
 *  - =, !=, <>, <, <=, >, >=
 *  - BETWEEN a AND b
 *  - IN (a, b, c)
 *  - LIKE 'pattern' (optional; uses reflection to avoid hard dependency)
 *  - literals: NULL, TRUE/FALSE, numbers, 'strings' ('' escape)
 *  - identifiers can be qualified (t.col) and quoted with "..." or `...`
 *
 * Note:
 *  - Map values are PropertyTerm<B, ? extends Serializable>, so we must erase wildcard capture
 *    at the callsite (unchecked cast) to call eq/lt/in/etc.
 */
public final class RocksJdbcWhereCompiler {
    private RocksJdbcWhereCompiler() {}

    public static <B extends Serializable> Condition<B> compile(
            String whereSql,
            Map<String, PropertyTerm<B, ? extends Serializable>> termsByColumnLower
    ) throws SQLException {
        if (whereSql == null || whereSql.isBlank()) return Conditions.trueCondition();
        Objects.requireNonNull(termsByColumnLower, "termsByColumnLower");

        List<Tok> toks = tokenize(whereSql);
        Parser<B> p = new Parser<>(toks, termsByColumnLower);

        Condition<B> c = p.parseExpr();
        p.expect(TokKind.EOF);

        return c == null ? Conditions.trueCondition() : c;
    }

    // tokenizer

    private enum TokKind { IDENT, STRING, NUMBER, OP, LP, RP, COMMA, EOF, KW }

    private record Tok(TokKind k, String s) {}

    private static List<Tok> tokenize(String s) throws SQLException {
        ArrayList<Tok> out = new ArrayList<>();
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) { i++; continue; }

            if (c == '(') { out.add(new Tok(TokKind.LP, "(")); i++; continue; }
            if (c == ')') { out.add(new Tok(TokKind.RP, ")")); i++; continue; }
            if (c == ',') { out.add(new Tok(TokKind.COMMA, ",")); i++; continue; }

            // string literal: '...'
            if (c == '\'') {
                int j = i + 1;
                StringBuilder sb = new StringBuilder();
                while (j < s.length()) {
                    char d = s.charAt(j);
                    if (d == '\'') {
                        if (j + 1 < s.length() && s.charAt(j + 1) == '\'') { // escaped ''
                            sb.append('\'');
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    sb.append(d);
                    j++;
                }
                if (j >= s.length() || s.charAt(j) != '\'') {
                    throw new SQLException("Unterminated string literal in WHERE");
                }
                out.add(new Tok(TokKind.STRING, sb.toString()));
                i = j + 1;
                continue;
            }

            // operators
            if ("=<>!".indexOf(c) >= 0) {
                String op;
                if (i + 1 < s.length()) {
                    String two = s.substring(i, i + 2);
                    if (two.equals("<=") || two.equals(">=") || two.equals("<>") || two.equals("!=")) {
                        op = two;
                        i += 2;
                        out.add(new Tok(TokKind.OP, op));
                        continue;
                    }
                }
                op = String.valueOf(c);
                i++;
                out.add(new Tok(TokKind.OP, op));
                continue;
            }

            // number (accept exponent)
            if (Character.isDigit(c) || (c == '-' && i + 1 < s.length() && Character.isDigit(s.charAt(i + 1)))) {
                int j = i + 1;
                while (j < s.length()) {
                    char d = s.charAt(j);
                    if (Character.isDigit(d) || d == '.' || d == 'e' || d == 'E' || d == '+' || d == '-') j++;
                    else break;
                }
                out.add(new Tok(TokKind.NUMBER, s.substring(i, j)));
                i = j;
                continue;
            }

            // identifier / keyword (allow dots)
            if (Character.isLetter(c) || c == '_' || c == '"' || c == '`') {
                String ident = readIdent(s, i);
                i += ident.length();
                String raw = stripQuotes(ident);
                String kw = raw.toLowerCase(Locale.ROOT);

                if (isKeyword(kw)) out.add(new Tok(TokKind.KW, kw));
                else out.add(new Tok(TokKind.IDENT, raw));
                continue;
            }

            throw new SQLException("Unexpected character in WHERE: '" + c + "'");
        }

        out.add(new Tok(TokKind.EOF, ""));
        return out;
    }

    private static boolean isKeyword(String kw) {
        return switch (kw) {
            case "and","or","not","is","null","in","between","like","true","false" -> true;
            default -> false;
        };
    }

    private static String readIdent(String s, int i) {
        int j = i;
        if (s.charAt(j) == '"' || s.charAt(j) == '`') {
            char q = s.charAt(j);
            j++;
            while (j < s.length() && s.charAt(j) != q) j++;
            if (j < s.length()) j++;
            return s.substring(i, j);
        }
        while (j < s.length()) {
            char c = s.charAt(j);
            if (Character.isLetterOrDigit(c) || c == '_' || c == '.') j++;
            else break;
        }
        return s.substring(i, j);
    }

    private static String stripQuotes(String s) {
        String t = s.trim();
        if (t.length() >= 2) {
            char a = t.charAt(0), b = t.charAt(t.length() - 1);
            if ((a == '"' && b == '"') || (a == '`' && b == '`')) return t.substring(1, t.length() - 1);
        }
        return t;
    }

    // parser

    private static final class Parser<B extends Serializable> {
        private final List<Tok> t;
        private final Map<String, PropertyTerm<B, ? extends Serializable>> terms;
        private int p = 0;

        Parser(List<Tok> toks, Map<String, PropertyTerm<B, ? extends Serializable>> terms) {
            this.t = toks;
            this.terms = terms;
        }

        Condition<B> parseExpr() throws SQLException { // OR
            Condition<B> left = parseAnd();
            while (peekKw("or")) {
                next();
                Condition<B> right = parseAnd();
                left = left.or(right);
            }
            return left;
        }

        private Condition<B> parseAnd() throws SQLException {
            Condition<B> left = parseUnary();
            while (peekKw("and")) {
                next();
                Condition<B> right = parseUnary();
                left = left.and(right);
            }
            return left;
        }

        private Condition<B> parseUnary() throws SQLException {
            if (peekKw("not")) {
                next();
                return parseUnary().not();
            }
            if (peek(TokKind.LP)) {
                next();
                Condition<B> inner = parseExpr();
                expect(TokKind.RP);
                return inner;
            }
            return parsePred();
        }

        private Condition<B> parsePred() throws SQLException {
            String col = expectIdent();
            PropertyTerm<B, ? extends Serializable> term0 = resolveTerm(col);
            if (term0 == null) throw new SQLException("Unknown column in WHERE: " + col);

            // Erase wildcard capture so we can call eq/lt/in/etc with Serializable literals.
            @SuppressWarnings({"rawtypes","unchecked"})
            PropertyTerm<B, Serializable> term = (PropertyTerm) term0;

            // IS [NOT] NULL
            if (peekKw("is")) {
                next();
                boolean not = false;
                if (peekKw("not")) { next(); not = true; }
                expectKw("null");
                return not ? term.isNotNull() : term.isNull();
            }

            // BETWEEN a AND b
            if (peekKw("between")) {
                next();
                Serializable a = readValue();
                expectKw("and");
                Serializable b = readValue();
                return term.between(a, b);
            }

            // IN (a,b,c)
            if (peekKw("in")) {
                next();
                expect(TokKind.LP);
                ArrayList<Serializable> vs = new ArrayList<>();
                if (!peek(TokKind.RP)) {
                    while (true) {
                        vs.add(readValue());
                        if (peek(TokKind.COMMA)) { next(); continue; }
                        break;
                    }
                }
                expect(TokKind.RP);
                Serializable[] arr = vs.toArray(new Serializable[0]);
                return term.in(arr);
            }

            // LIKE 'pattern'
            if (peekKw("like")) {
                next();
                Serializable v = readValue();
                if (!(v instanceof String s)) throw new SQLException("LIKE expects string literal");
                return term.like(s);
            }

            // Comparison: = != <> < <= > >=
            String op = expectOp();
            Serializable v = readValue();

            return switch (op) {
                case "="  -> term.eq(v);
                case "!=" , "<>" -> term.ne(v);
                case "<"  -> term.lt(v);
                case "<=" -> term.le(v);
                case ">"  -> term.gt(v);
                case ">=" -> term.ge(v);
                default -> throw new SQLException("Unsupported operator: " + op);
            };
        }

        private PropertyTerm<B, ? extends Serializable> resolveTerm(String ident) {
            String raw = ident;
            int dot = raw.lastIndexOf('.');
            if (dot >= 0) raw = raw.substring(dot + 1);
            String k = stripQuotes(raw).trim().toLowerCase(Locale.ROOT);
            return terms.get(k);
        }

        private Serializable readValue() throws SQLException {
            if (peekKw("null")) { next(); return null; }
            if (peekKw("true")) { next(); return Boolean.TRUE; }
            if (peekKw("false")) { next(); return Boolean.FALSE; }

            Tok x = peek();
            if (x.k == TokKind.STRING) { next(); return coerceString(x.s); }
            if (x.k == TokKind.NUMBER) { next(); return coerceNumber(x.s); }

            throw new SQLException("Expected literal value, got: " + x.k + " '" + x.s + "'");
        }

        private static Serializable coerceString(String s) {
            try { return Timestamp.valueOf(s); } catch (Throwable ignored) {}
            try { return LocalDateTime.parse(s); } catch (Throwable ignored) {}
            try { return LocalDate.parse(s); } catch (Throwable ignored) {}
            return s;
        }

        private static Serializable coerceNumber(String s) {
            String t = s.trim();
            if (t.indexOf('.') >= 0 || t.indexOf('e') >= 0 || t.indexOf('E') >= 0) {
                try { return Double.parseDouble(t); } catch (Throwable ignored) {}
                try { return new BigDecimal(t); } catch (Throwable ignored) {}
                return t;
            }
            try {
                long v = Long.parseLong(t);
                if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) return (int) v;
                return v;
            } catch (Throwable ignored) {}
            try { return new BigDecimal(t); } catch (Throwable ignored) {}
            return t;
        }

        private boolean peekKw(String kw) {
            Tok x = peek();
            return x.k == TokKind.KW && x.s.equals(kw);
        }

        private void expectKw(String kw) throws SQLException {
            Tok x = peek();
            if (x.k == TokKind.KW && x.s.equals(kw)) { next(); return; }
            throw new SQLException("Expected keyword " + kw + " but got: " + x.k + " '" + x.s + "'");
        }

        private String expectIdent() throws SQLException {
            Tok x = peek();
            if (x.k != TokKind.IDENT) throw new SQLException("Expected column identifier, got: " + x.k + " '" + x.s + "'");
            next();
            return x.s;
        }

        private String expectOp() throws SQLException {
            Tok x = peek();
            if (x.k != TokKind.OP) throw new SQLException("Expected operator, got: " + x.k + " '" + x.s + "'");
            next();
            return x.s;
        }

        private boolean peek(TokKind k) { return peek().k == k; }
        private Tok peek() { return t.get(p); }
        private Tok next() { return t.get(p++); }

        void expect(TokKind k) throws SQLException {
            Tok x = peek();
            if (x.k != k) throw new SQLException("Expected " + k + " but got: " + x.k + " '" + x.s + "'");
            next();
        }
    }
}
