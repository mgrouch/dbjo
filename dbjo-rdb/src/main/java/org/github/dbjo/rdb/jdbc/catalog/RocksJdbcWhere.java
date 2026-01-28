package org.github.dbjo.rdb.jdbc.catalog;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public final class RocksJdbcWhere {
    private RocksJdbcWhere() {}

    // -------- AST --------

    public interface Expr {}

    public record And(Expr left, Expr right) implements Expr {}
    public record Or(Expr left, Expr right) implements Expr {}
    public record Not(Expr inner) implements Expr {}

    public interface Pred extends Expr {}

    public record IsNull(String col, boolean not) implements Pred {}
    public record Between(String col, Serializable a, Serializable b) implements Pred {}
    public record In(String col, List<Serializable> values) implements Pred {}
    public record Cmp(String col, Op op, Serializable value) implements Pred {}

    public enum Op { EQ, NE, LT, LE, GT, GE }

    public static Expr parse(String whereSql) throws SQLException {
        if (whereSql == null || whereSql.isBlank()) return null;
        List<Tok> toks = tokenize(whereSql);
        Parser p = new Parser(toks);
        Expr e = p.parseExpr();
        p.expect(TokKind.EOF);
        return e;
    }

    // ---------------- tokenizer ----------------

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

            if (c == '\'') {
                int j = i + 1;
                StringBuilder sb = new StringBuilder();
                while (j < s.length()) {
                    char d = s.charAt(j);
                    if (d == '\'') {
                        if (j + 1 < s.length() && s.charAt(j + 1) == '\'') { sb.append('\''); j += 2; continue; }
                        break;
                    }
                    sb.append(d);
                    j++;
                }
                if (j >= s.length() || s.charAt(j) != '\'') throw new SQLException("Unterminated string literal in WHERE");
                out.add(new Tok(TokKind.STRING, sb.toString()));
                i = j + 1;
                continue;
            }

            if ("=<>!".indexOf(c) >= 0) {
                if (i + 1 < s.length()) {
                    String two = s.substring(i, i + 2);
                    if (two.equals("<=") || two.equals(">=") || two.equals("<>") || two.equals("!=")) {
                        out.add(new Tok(TokKind.OP, two)); i += 2; continue;
                    }
                }
                out.add(new Tok(TokKind.OP, String.valueOf(c))); i++; continue;
            }

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
            case "and","or","not","is","null","in","between","true","false" -> true;
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
            char a = t.charAt(0), b = t.charAt(t.length()-1);
            if ((a == '"' && b == '"') || (a == '`' && b == '`')) return t.substring(1, t.length()-1);
        }
        return t;
    }

    // ---------------- parser ----------------

    private static final class Parser {
        private final List<Tok> t;
        private int p = 0;

        Parser(List<Tok> toks) { this.t = toks; }

        Expr parseExpr() throws SQLException { // OR
            Expr left = parseAnd();
            while (peekKw("or")) {
                next();
                Expr right = parseAnd();
                left = new Or(left, right);
            }
            return left;
        }

        private Expr parseAnd() throws SQLException {
            Expr left = parseUnary();
            while (peekKw("and")) {
                next();
                Expr right = parseUnary();
                left = new And(left, right);
            }
            return left;
        }

        private Expr parseUnary() throws SQLException {
            if (peekKw("not")) {
                next();
                return new Not(parseUnary());
            }
            if (peek(TokKind.LP)) {
                next();
                Expr inner = parseExpr();
                expect(TokKind.RP);
                return inner;
            }
            return parsePred();
        }

        private Expr parsePred() throws SQLException {
            String col = expectIdent();

            if (peekKw("is")) {
                next();
                boolean not = false;
                if (peekKw("not")) { next(); not = true; }
                expectKw("null");
                return new IsNull(col, not);
            }

            if (peekKw("between")) {
                next();
                Serializable a = (Serializable) readValue();
                expectKw("and");
                Serializable b = (Serializable) readValue();
                return new Between(col, a, b);
            }

            if (peekKw("in")) {
                next();
                expect(TokKind.LP);
                ArrayList<Serializable> vs = new ArrayList<>();
                if (!peek(TokKind.RP)) {
                    while (true) {
                        vs.add((Serializable) readValue());
                        if (peek(TokKind.COMMA)) { next(); continue; }
                        break;
                    }
                }
                expect(TokKind.RP);
                return new In(col, vs);
            }

            String op = expectOp();
            Serializable v = (Serializable) readValue();

            if (op.equals("="))  return new Cmp(col, Op.EQ, v);
            if (op.equals("!=") || op.equals("<>")) return new Cmp(col, Op.NE, v);
            if (op.equals("<"))  return new Cmp(col, Op.LT, v);
            if (op.equals("<=")) return new Cmp(col, Op.LE, v);
            if (op.equals(">"))  return new Cmp(col, Op.GT, v);
            if (op.equals(">=")) return new Cmp(col, Op.GE, v);

            throw new SQLException("Unsupported operator: " + op);
        }

        private Object readValue() throws SQLException {
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
