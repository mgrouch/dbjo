package org.github.dbjo.rdb.jdbc.catalog;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Small WHERE parser for:
 *   - col = 'x' | col != 1 | col < 2 | col <= 3 | col > 4 | col >= 5
 *   - col BETWEEN a AND b
 *   - col IN (a,b,c)
 *   - IS NULL / IS NOT NULL
 *   - AND / OR with parentheses
 */
public final class RocksJdbcWhereParser {
    private RocksJdbcWhereParser() {}

    // --- AST ---

    public sealed interface Expr permits And, Or, Not, Cmp, Between, In, IsNull, True {
        boolean eval(Row row) throws SQLException;
    }

    public record True() implements Expr {
        @Override public boolean eval(Row row) { return true; }
    }

    public record And(Expr left, Expr right) implements Expr {
        @Override public boolean eval(Row row) throws SQLException { return left.eval(row) && right.eval(row); }
    }

    public record Or(Expr left, Expr right) implements Expr {
        @Override public boolean eval(Row row) throws SQLException { return left.eval(row) || right.eval(row); }
    }

    public record Not(Expr inner) implements Expr {
        @Override public boolean eval(Row row) throws SQLException { return !inner.eval(row); }
    }

    public enum Op { EQ, NE, LT, LE, GT, GE }

    public record Cmp(String col, Op op, Lit lit) implements Expr {
        @Override public boolean eval(Row row) throws SQLException {
            Object v = row.get(col);
            Object r = lit.value();
            return compare(v, op, r);
        }
    }

    public record Between(String col, Lit lo, Lit hi, boolean negated) implements Expr {
        @Override public boolean eval(Row row) throws SQLException {
            Object v = row.get(col);
            boolean ok = compare(v, Op.GE, lo.value()) && compare(v, Op.LE, hi.value());
            return negated ? !ok : ok;
        }
    }

    public record In(String col, List<Lit> values, boolean negated) implements Expr {
        @Override public boolean eval(Row row) throws SQLException {
            Object v = row.get(col);
            boolean ok = false;
            for (Lit l : values) {
                if (equalsValue(v, l.value())) { ok = true; break; }
            }
            return negated ? !ok : ok;
        }
    }

    public record IsNull(String col, boolean negated) implements Expr {
        @Override public boolean eval(Row row) throws SQLException {
            Object v = row.get(col);
            boolean ok = (v == null);
            return negated ? !ok : ok;
        }
    }

    public interface Row {
        Object get(String colName) throws SQLException;
    }

    public sealed interface Lit permits LitNull, LitString, LitNumber, LitBoolean {
        Object value();
    }
    public record LitNull() implements Lit { @Override public Object value() { return null; } }
    public record LitString(String s) implements Lit { @Override public Object value() { return s; } }
    public record LitNumber(BigDecimal n) implements Lit { @Override public Object value() { return n; } }
    public record LitBoolean(boolean b) implements Lit { @Override public Object value() { return b; } }

    // --- parse entry ---

    public static Expr parse(String whereSql) throws SQLException {
        if (whereSql == null || whereSql.isBlank()) return new True();
        Tokenizer tz = new Tokenizer(whereSql);
        Parser p = new Parser(tz);
        Expr e = p.parseExpr();
        p.expect(TokenType.EOF);
        return e;
    }

    // --- evaluation helpers ---

    private static boolean equalsValue(Object a, Object b) {
        if (a == null || b == null) return a == b;
        if (a instanceof Number || b instanceof Number) {
            BigDecimal da = toDecimalOrNull(a);
            BigDecimal db = toDecimalOrNull(b);
            if (da != null && db != null) return da.compareTo(db) == 0;
        }
        return a.equals(b);
    }

    private static boolean compare(Object left, Op op, Object right) throws SQLException {
        if (op == Op.EQ) return equalsValue(left, right);
        if (op == Op.NE) return !equalsValue(left, right);

        if (left == null || right == null) return false;

        // numeric compare
        BigDecimal dl = toDecimalOrNull(left);
        BigDecimal dr = toDecimalOrNull(right);
        if (dl != null && dr != null) {
            int c = dl.compareTo(dr);
            return switch (op) {
                case LT -> c < 0;
                case LE -> c <= 0;
                case GT -> c > 0;
                case GE -> c >= 0;
                default -> false;
            };
        }

        // string compare
        if (left instanceof String ls) {
            String rs = String.valueOf(right);
            int c = ls.compareTo(rs);
            return switch (op) {
                case LT -> c < 0;
                case LE -> c <= 0;
                case GT -> c > 0;
                case GE -> c >= 0;
                default -> false;
            };
        }

        // comparable same-type
        if (left instanceof Comparable<?> cLeft && left.getClass().isInstance(right)) {
            @SuppressWarnings("unchecked")
            int c = ((Comparable<Object>) cLeft).compareTo(right);
            return switch (op) {
                case LT -> c < 0;
                case LE -> c <= 0;
                case GT -> c > 0;
                case GE -> c >= 0;
                default -> false;
            };
        }

        // fallback to string compare
        int c = String.valueOf(left).compareTo(String.valueOf(right));
        return switch (op) {
            case LT -> c < 0;
            case LE -> c <= 0;
            case GT -> c > 0;
            case GE -> c >= 0;
            default -> false;
        };
    }

    private static BigDecimal toDecimalOrNull(Object o) {
        if (o == null) return null;
        if (o instanceof BigDecimal bd) return bd;
        if (o instanceof Number n) {
            try {
                return new BigDecimal(String.valueOf(n));
            } catch (Exception ignore) {
                return null;
            }
        }
        if (o instanceof String s) {
            try {
                return new BigDecimal(s.trim());
            } catch (Exception ignore) {
                return null;
            }
        }
        return null;
    }

    // --- tokenizer / parser ---

    enum TokenType { IDENT, STRING, NUMBER, LP, RP, COMMA, OP, AND, OR, NOT, BETWEEN, IN, IS, NULL, TRUE, FALSE, EOF }

    record Token(TokenType t, String s) {}

    static final class Tokenizer {
        private final String src;
        private int i = 0;

        Tokenizer(String src) { this.src = src; }

        Token next() throws SQLException {
            skipWs();
            if (i >= src.length()) return new Token(TokenType.EOF, "");
            char c = src.charAt(i);

            if (c == '(') { i++; return new Token(TokenType.LP, "("); }
            if (c == ')') { i++; return new Token(TokenType.RP, ")"); }
            if (c == ',') { i++; return new Token(TokenType.COMMA, ","); }

            // string literal
            if (c == '\'') {
                i++;
                StringBuilder sb = new StringBuilder();
                while (i < src.length()) {
                    char d = src.charAt(i++);
                    if (d == '\'') {
                        if (i < src.length() && src.charAt(i) == '\'') { // escaped ''
                            i++;
                            sb.append('\'');
                            continue;
                        }
                        break;
                    }
                    sb.append(d);
                }
                return new Token(TokenType.STRING, sb.toString());
            }

            // operators: <= >= <> != = < >
            if ("=<>!".indexOf(c) >= 0) {
                String op;
                if (i + 1 < src.length()) {
                    String two = src.substring(i, i + 2);
                    if (two.equals("<=") || two.equals(">=") || two.equals("<>") || two.equals("!=")) {
                        op = two;
                        i += 2;
                        return new Token(TokenType.OP, op);
                    }
                }
                op = String.valueOf(c);
                i++;
                return new Token(TokenType.OP, op);
            }

            // number (simple)
            if (Character.isDigit(c) || (c == '-' && i + 1 < src.length() && Character.isDigit(src.charAt(i + 1)))) {
                int j = i + 1;
                while (j < src.length()) {
                    char d = src.charAt(j);
                    if (!(Character.isDigit(d) || d == '.' || d == 'e' || d == 'E' || d == '+' || d == '-')) break;
                    j++;
                }
                String num = src.substring(i, j);
                i = j;
                return new Token(TokenType.NUMBER, num);
            }

            // identifier / keyword
            if (Character.isLetter(c) || c == '_' || c == '"' || c == '[' || c == '`') {
                String ident = readIdent();
                String u = ident.toUpperCase(Locale.ROOT);
                return switch (u) {
                    case "AND" -> new Token(TokenType.AND, ident);
                    case "OR" -> new Token(TokenType.OR, ident);
                    case "NOT" -> new Token(TokenType.NOT, ident);
                    case "BETWEEN" -> new Token(TokenType.BETWEEN, ident);
                    case "IN" -> new Token(TokenType.IN, ident);
                    case "IS" -> new Token(TokenType.IS, ident);
                    case "NULL" -> new Token(TokenType.NULL, ident);
                    case "TRUE" -> new Token(TokenType.TRUE, ident);
                    case "FALSE" -> new Token(TokenType.FALSE, ident);
                    default -> new Token(TokenType.IDENT, ident);
                };
            }

            throw new SQLException("Bad WHERE token at " + i + ": " + c);
        }

        private String readIdent() throws SQLException {
            char c = src.charAt(i);

            // quoted identifiers: "x"  [x]  `x`
            if (c == '"' || c == '[' || c == '`') {
                char open = c;
                char close = (open == '[') ? ']' : open;
                i++;
                int start = i;
                while (i < src.length() && src.charAt(i) != close) i++;
                if (i >= src.length()) throw new SQLException("Unclosed identifier quote");
                String out = src.substring(start, i);
                i++; // consume close
                return out;
            }

            int start = i;
            i++;
            while (i < src.length()) {
                char d = src.charAt(i);
                if (!(Character.isLetterOrDigit(d) || d == '_' || d == '.')) break;
                i++;
            }
            String out = src.substring(start, i);
            // keep only last segment for schema-qualified
            int dot = out.lastIndexOf('.');
            return (dot >= 0) ? out.substring(dot + 1) : out;
        }

        private void skipWs() {
            while (i < src.length() && Character.isWhitespace(src.charAt(i))) i++;
        }
    }

    static final class Parser {
        private final Tokenizer tz;
        private Token look;

        Parser(Tokenizer tz) throws SQLException {
            this.tz = tz;
            this.look = tz.next();
        }

        Expr parseExpr() throws SQLException { return parseOr(); }

        private Expr parseOr() throws SQLException {
            Expr e = parseAnd();
            while (look.t == TokenType.OR) {
                consume();
                e = new Or(e, parseAnd());
            }
            return e;
        }

        private Expr parseAnd() throws SQLException {
            Expr e = parseUnary();
            while (look.t == TokenType.AND) {
                consume();
                e = new And(e, parseUnary());
            }
            return e;
        }

        private Expr parseUnary() throws SQLException {
            if (look.t == TokenType.NOT) {
                consume();
                return new Not(parseUnary());
            }
            if (look.t == TokenType.LP) {
                consume();
                Expr e = parseExpr();
                expect(TokenType.RP);
                return e;
            }
            return parsePredicate();
        }

        private Expr parsePredicate() throws SQLException {
            String col = expectIdent();
            // IS [NOT] NULL
            if (look.t == TokenType.IS) {
                consume();
                boolean neg = false;
                if (look.t == TokenType.NOT) { neg = true; consume(); }
                expect(TokenType.NULL);
                return new IsNull(col, neg);
            }

            // BETWEEN / NOT BETWEEN
            boolean negBetween = false;
            if (look.t == TokenType.NOT) { negBetween = true; consume(); }
            if (look.t == TokenType.BETWEEN) {
                consume();
                Lit lo = parseLit();
                expectIdentKeyword("AND");
                Lit hi = parseLit();
                return new Between(col, lo, hi, negBetween);
            }

            // IN / NOT IN
            boolean negIn = false;
            if (look.t == TokenType.NOT) { negIn = true; consume(); }
            if (look.t == TokenType.IN) {
                consume();
                expect(TokenType.LP);
                ArrayList<Lit> vs = new ArrayList<>();
                if (look.t != TokenType.RP) {
                    vs.add(parseLit());
                    while (look.t == TokenType.COMMA) { consume(); vs.add(parseLit()); }
                }
                expect(TokenType.RP);
                return new In(col, vs, negIn);
            }

            // comparison
            if (look.t != TokenType.OP) throw new SQLException("Expected operator after column: " + col);
            String op = look.s;
            consume();
            Lit lit = parseLit();
            return new Cmp(col, toOp(op), lit);
        }

        private Op toOp(String op) throws SQLException {
            return switch (op) {
                case "=" -> Op.EQ;
                case "!=" , "<>" -> Op.NE;
                case "<" -> Op.LT;
                case "<=" -> Op.LE;
                case ">" -> Op.GT;
                case ">=" -> Op.GE;
                default -> throw new SQLException("Bad operator: " + op);
            };
        }

        private Lit parseLit() throws SQLException {
            return switch (look.t) {
                case NULL -> { consume(); yield new LitNull(); }
                case STRING -> { String s = look.s; consume(); yield new LitString(s); }
                case NUMBER -> {
                    String s = look.s;
                    consume();
                    try { yield new LitNumber(new BigDecimal(s)); }
                    catch (Exception e) { throw new SQLException("Bad number: " + s); }
                }
                case TRUE -> { consume(); yield new LitBoolean(true); }
                case FALSE -> { consume(); yield new LitBoolean(false); }
                default -> throw new SQLException("Expected literal, got: " + look.t);
            };
        }

        private void expectIdentKeyword(String kw) throws SQLException {
            if (look.t == TokenType.IDENT && look.s.equalsIgnoreCase(kw)) { consume(); return; }
            if (look.t.name().equals(kw)) { consume(); return; }
            throw new SQLException("Expected " + kw);
        }

        private String expectIdent() throws SQLException {
            if (look.t != TokenType.IDENT) throw new SQLException("Expected identifier");
            String s = look.s;
            consume();
            return s;
        }

        void expect(TokenType t) throws SQLException {
            if (look.t != t) throw new SQLException("Expected " + t + " got " + look.t);
            consume();
        }

        void consume() throws SQLException { look = tz.next(); }
    }
}
