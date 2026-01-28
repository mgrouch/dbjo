package org.github.dbjo.rdb.jdbc.catalog;

import org.github.dbjo.criteria.Condition;
import org.github.dbjo.criteria.Conditions;
import org.github.dbjo.criteria.PropertyTerm;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

/**
 * Compile SQL WHERE clause into dbjo criteria Condition using generated *Q terms.
 *
 * Supported:
 *  - =, !=, <>, <, <=, >, >=
 *  - BETWEEN a AND b
 *  - IN (a,b,c)
 *  - IS NULL / IS NOT NULL
 *  - AND / OR / NOT with parentheses
 *
 * If a column is unknown (not present in termsByLowerName), throws SQLException.
 */
public final class RocksJdbcCriteriaCompiler {
    private RocksJdbcCriteriaCompiler() {}

    public static <B extends Serializable> Condition<B> compile(
            String whereSql,
            Map<String, PropertyTerm<B, ? extends Serializable>> termsByLowerName
    ) throws SQLException {
        if (whereSql == null || whereSql.isBlank()) {
            return Conditions.trueCondition();
        }
        Objects.requireNonNull(termsByLowerName, "termsByLowerName");

        Lexer lx = new Lexer(whereSql);
        Parser<B> p = new Parser<>(lx.tokenize(), termsByLowerName);
        Condition<B> c = p.parseExpr();
        p.expect(TokenKind.EOF);
        return c;
    }

    // ---------------- tokenizer ----------------

    enum TokenKind {
        IDENT, STRING, NUMBER, TRUE, FALSE, NULL,
        EQ, NE, LT, LE, GT, GE,
        LP, RP, COMMA,
        AND, OR, NOT,
        IN, BETWEEN, IS,
        EOF
    }

    record Tok(TokenKind k, String s) {}

    static final class Lexer {
        private final String src;
        private int i;

        Lexer(String src) { this.src = src; }

        List<Tok> tokenize() throws SQLException {
            ArrayList<Tok> out = new ArrayList<>();
            while (true) {
                skipWs();
                if (i >= src.length()) { out.add(new Tok(TokenKind.EOF, "")); return out; }

                char c = src.charAt(i);

                if (c == '(') { i++; out.add(new Tok(TokenKind.LP, "(")); continue; }
                if (c == ')') { i++; out.add(new Tok(TokenKind.RP, ")")); continue; }
                if (c == ',') { i++; out.add(new Tok(TokenKind.COMMA, ",")); continue; }

                if (c == '=') { i++; out.add(new Tok(TokenKind.EQ, "=")); continue; }
                if (c == '<') {
                    if (peek('=', 1)) { i+=2; out.add(new Tok(TokenKind.LE, "<=")); continue; }
                    if (peek('>', 1)) { i+=2; out.add(new Tok(TokenKind.NE, "<>")); continue; }
                    i++; out.add(new Tok(TokenKind.LT, "<")); continue;
                }
                if (c == '>') {
                    if (peek('=', 1)) { i+=2; out.add(new Tok(TokenKind.GE, ">=")); continue; }
                    i++; out.add(new Tok(TokenKind.GT, ">")); continue;
                }
                if (c == '!') {
                    if (peek('=', 1)) { i+=2; out.add(new Tok(TokenKind.NE, "!=")); continue; }
                    throw err("Unexpected '!'");
                }

                if (c == '\'') {
                    out.add(new Tok(TokenKind.STRING, readString()));
                    continue;
                }

                if (Character.isDigit(c) || (c == '-' && i+1 < src.length() && Character.isDigit(src.charAt(i+1)))) {
                    out.add(new Tok(TokenKind.NUMBER, readNumber()));
                    continue;
                }

                out.add(readIdentOrKeyword(out));
            }
        }

        private void skipWs() {
            while (i < src.length() && Character.isWhitespace(src.charAt(i))) i++;
        }

        private boolean peek(char c, int off) {
            int j = i + off;
            return j < src.length() && src.charAt(j) == c;
        }

        private String readString() throws SQLException {
            i++; // '
            StringBuilder sb = new StringBuilder();
            while (i < src.length()) {
                char c = src.charAt(i);
                if (c == '\'') {
                    if (i+1 < src.length() && src.charAt(i+1) == '\'') {
                        sb.append('\''); i += 2; continue;
                    }
                    i++;
                    return sb.toString();
                }
                sb.append(c);
                i++;
            }
            throw err("Unterminated string literal");
        }

        private String readNumber() {
            int j = i;
            if (src.charAt(i) == '-') i++;
            while (i < src.length() && Character.isDigit(src.charAt(i))) i++;
            if (i < src.length() && src.charAt(i) == '.') {
                i++;
                while (i < src.length() && Character.isDigit(src.charAt(i))) i++;
            }
            if (i < src.length()) {
                char c = src.charAt(i);
                if (c == 'e' || c == 'E') {
                    i++;
                    if (i < src.length() && (src.charAt(i) == '+' || src.charAt(i) == '-')) i++;
                    while (i < src.length() && Character.isDigit(src.charAt(i))) i++;
                }
            }
            return src.substring(j, i);
        }

        private Tok readIdentOrKeyword(List<Tok> out) throws SQLException {
            // quoted ident "x" `x` [x]
            char c = src.charAt(i);
            if (c == '"' || c == '`' || c == '[') {
                char end = (c == '[') ? ']' : c;
                i++;
                int start = i;
                while (i < src.length() && src.charAt(i) != end) i++;
                if (i >= src.length()) throw err("Unterminated quoted identifier");
                String body = src.substring(start, i);
                i++;
                return new Tok(TokenKind.IDENT, body);
            }

            int j = i;
            while (i < src.length()) {
                char ch = src.charAt(i);
                if (Character.isLetterOrDigit(ch) || ch == '_' || ch == '.') i++;
                else break;
            }

            String raw = src.substring(j, i);
            String u = raw.toUpperCase(Locale.ROOT);

            return switch (u) {
                case "AND" -> new Tok(TokenKind.AND, raw);
                case "OR" -> new Tok(TokenKind.OR, raw);
                case "NOT" -> new Tok(TokenKind.NOT, raw);
                case "IN" -> new Tok(TokenKind.IN, raw);
                case "BETWEEN" -> new Tok(TokenKind.BETWEEN, raw);
                case "IS" -> new Tok(TokenKind.IS, raw);
                case "TRUE" -> new Tok(TokenKind.TRUE, raw);
                case "FALSE" -> new Tok(TokenKind.FALSE, raw);
                case "NULL" -> new Tok(TokenKind.NULL, raw);
                default -> new Tok(TokenKind.IDENT, raw);
            };
        }

        private SQLException err(String msg) {
            return new SQLException("WHERE parse error at pos " + i + ": " + msg + " in: " + src);
        }
    }

    // ---------------- parser ----------------

    static final class Parser<B extends Serializable> {
        private final List<Tok> toks;
        private final Map<String, PropertyTerm<B, ? extends Serializable>> terms;
        private int p = 0;

        Parser(List<Tok> toks, Map<String, PropertyTerm<B, ? extends Serializable>> terms) {
            this.toks = toks;
            this.terms = terms;
        }

        Condition<B> parseExpr() throws SQLException { return parseOr(); }

        private Condition<B> parseOr() throws SQLException {
            Condition<B> left = parseAnd();
            while (match(TokenKind.OR)) {
                Condition<B> right = parseAnd();
                left = left.or(right);
            }
            return left;
        }

        private Condition<B> parseAnd() throws SQLException {
            Condition<B> left = parseNot();
            while (match(TokenKind.AND)) {
                Condition<B> right = parseNot();
                left = left.and(right);
            }
            return left;
        }

        private Condition<B> parseNot() throws SQLException {
            if (match(TokenKind.NOT)) return parseNot().not();
            return parsePrimary();
        }

        private Condition<B> parsePrimary() throws SQLException {
            if (match(TokenKind.LP)) {
                Condition<B> inner = parseExpr();
                expect(TokenKind.RP);
                return inner;
            }
            return parsePredicate();
        }

        @SuppressWarnings({"rawtypes","unchecked"})
        private Condition<B> parsePredicate() throws SQLException {
            String col = expectIdent();
            PropertyTerm term = resolveTerm(col);

            // IS [NOT] NULL
            if (match(TokenKind.IS)) {
                boolean not = match(TokenKind.NOT);
                expect(TokenKind.NULL);
                return not ? term.isNotNull() : term.isNull();
            }

            // [NOT] IN (...)
            if (match(TokenKind.NOT)) {
                expect(TokenKind.IN);
                Condition<B> in = term.in(parseInList());
                return in.not();
            }
            if (match(TokenKind.IN)) {
                return term.in(parseInList());
            }

            // BETWEEN a AND b
            if (match(TokenKind.BETWEEN)) {
                Serializable lo = parseLiteral();
                expect(TokenKind.AND);
                Serializable hi = parseLiteral();
                return term.between(lo, hi);
            }

            TokenKind op = next().k();
            if (!(op == TokenKind.EQ || op == TokenKind.NE || op == TokenKind.LT || op == TokenKind.LE || op == TokenKind.GT || op == TokenKind.GE)) {
                throw err("Expected comparison operator after column: " + col);
            }

            Serializable lit = parseLiteral();

            return switch (op) {
                case EQ -> term.eq(lit);
                case NE -> term.ne(lit);
                case LT -> term.lt(lit);
                case LE -> term.le(lit);
                case GT -> term.gt(lit);
                case GE -> term.ge(lit);
                default -> throw err("Bad operator");
            };
        }

        private List<Serializable> parseInList() throws SQLException {
            expect(TokenKind.LP);
            ArrayList<Serializable> vals = new ArrayList<>();
            if (!peek(TokenKind.RP)) {
                vals.add(parseLiteral());
                while (match(TokenKind.COMMA)) vals.add(parseLiteral());
            }
            expect(TokenKind.RP);
            return vals;
        }

        private Serializable parseLiteral() throws SQLException {
            Tok t = next();
            return switch (t.k()) {
                case STRING -> t.s();
                case NUMBER -> parseNumber(t.s());
                case TRUE -> Boolean.TRUE;
                case FALSE -> Boolean.FALSE;
                case NULL -> null;
                default -> throw err("Expected literal, got: " + t.k());
            };
        }

        private Serializable parseNumber(String s) throws SQLException {
            try {
                if (s.indexOf('.') >= 0 || s.indexOf('e') >= 0 || s.indexOf('E') >= 0) return Double.valueOf(s);
                long v = Long.parseLong(s);
                if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) return (int) v;
                return v;
            } catch (NumberFormatException e) {
                throw err("Bad number literal: " + s);
            }
        }

        private PropertyTerm<B, ? extends Serializable> resolveTerm(String ident) throws SQLException {
            String base = ident;
            int dot = base.lastIndexOf('.');
            if (dot >= 0) base = base.substring(dot + 1);

            String key = stripQuotes(base).trim().toLowerCase(Locale.ROOT);
            PropertyTerm<B, ? extends Serializable> t = terms.get(key);
            if (t == null) throw err("Unknown column in WHERE: " + base);
            return t;
        }

        private static String stripQuotes(String s) {
            String t = s.trim();
            if (t.length() >= 2) {
                if ((t.startsWith("\"") && t.endsWith("\"")) || (t.startsWith("`") && t.endsWith("`"))) {
                    return t.substring(1, t.length() - 1);
                }
            }
            return t;
        }

        private String expectIdent() throws SQLException {
            Tok t = next();
            if (t.k() != TokenKind.IDENT) throw err("Expected identifier");
            if (t.s() == null || t.s().isBlank()) throw err("Empty identifier");
            return t.s();
        }

        private boolean match(TokenKind k) {
            if (peek(k)) { p++; return true; }
            return false;
        }

        private boolean peek(TokenKind k) {
            return toks.get(p).k() == k;
        }

        private Tok next() throws SQLException {
            Tok t = toks.get(p);
            if (t.k() == TokenKind.EOF) throw err("Unexpected end of WHERE");
            p++;
            return t;
        }

        void expect(TokenKind k) throws SQLException {
            Tok t = toks.get(p);
            if (t.k() != k) throw err("Expected " + k + " but got " + t.k());
            p++;
        }

        private SQLException err(String msg) {
            return new SQLException("WHERE compile error near token#" + p + ": " + msg);
        }
    }
}
