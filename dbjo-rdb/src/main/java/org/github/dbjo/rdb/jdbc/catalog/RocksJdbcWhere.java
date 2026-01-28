package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

public final class RocksJdbcWhere {
    private RocksJdbcWhere() {}

    public interface RowAccessor {
        Object get(int colIndex) throws SQLException;
    }

    public interface Predicate {
        boolean test(RowAccessor row) throws SQLException;
        static Predicate alwaysTrue() { return row -> true; }
    }

    public static Predicate compile(String whereSql, String[] colNames) throws SQLException {
        if (whereSql == null || whereSql.isBlank()) return Predicate.alwaysTrue();
        Tokenizer tz = new Tokenizer(whereSql);
        Parser p = new Parser(tz, colNames);
        Predicate pred = p.parseExpr();
        tz.expect(TokenKind.EOF);
        return pred;
    }

    enum TokenKind { IDENT, STRING, NUMBER, BOOL, NULL, OP, AND, OR, NOT, IS, LIKE, IN, LPAREN, RPAREN, COMMA, EOF }

    record Token(TokenKind kind, String text) {}

    static final class Tokenizer {
        private final String s;
        private int i = 0;
        private Token look;

        Tokenizer(String s) { this.s = s; }

        Token peek() throws SQLException {
            if (look == null) look = next0();
            return look;
        }

        Token next() throws SQLException {
            Token t = peek();
            look = null;
            return t;
        }

        void expect(TokenKind k) throws SQLException {
            Token t = next();
            if (t.kind != k) throw new SQLException("Expected " + k + " but got " + t.kind + " (" + t.text + ")");
        }

        boolean accept(TokenKind k) throws SQLException {
            if (peek().kind == k) { next(); return true; }
            return false;
        }

        private Token next0() throws SQLException {
            skipWs();
            if (i >= s.length()) return new Token(TokenKind.EOF, "");

            char c = s.charAt(i);

            if (c == '(') { i++; return new Token(TokenKind.LPAREN, "("); }
            if (c == ')') { i++; return new Token(TokenKind.RPAREN, ")"); }
            if (c == ',') { i++; return new Token(TokenKind.COMMA, ","); }

            if ("=<>!".indexOf(c) >= 0) {
                int j = i + 1;
                if (j < s.length()) {
                    char d = s.charAt(j);
                    if ((c == '<' && d == '=') || (c == '>' && d == '=') || (c == '!' && d == '=')
                            || (c == '<' && d == '>')) {
                        String op = s.substring(i, j + 1);
                        i = j + 1;
                        return new Token(TokenKind.OP, op);
                    }
                }
                i++;
                return new Token(TokenKind.OP, String.valueOf(c));
            }

            if (c == '\'') {
                StringBuilder sb = new StringBuilder();
                i++;
                while (i < s.length()) {
                    char x = s.charAt(i++);
                    if (x == '\'') {
                        if (i < s.length() && s.charAt(i) == '\'') { i++; sb.append('\''); continue; }
                        return new Token(TokenKind.STRING, sb.toString());
                    }
                    sb.append(x);
                }
                throw new SQLException("Unterminated string literal");
            }

            if (Character.isDigit(c) || (c == '.' && i + 1 < s.length() && Character.isDigit(s.charAt(i + 1)))) {
                int j = i;
                while (j < s.length()) {
                    char x = s.charAt(j);
                    if (Character.isDigit(x) || x == '.' || x == 'e' || x == 'E' || x == '+' || x == '-') j++;
                    else break;
                }
                String num = s.substring(i, j);
                i = j;
                return new Token(TokenKind.NUMBER, num);
            }

            if (c == '"' || c == '`' || c == '[' || Character.isLetter(c) || c == '_' ) {
                String ident = readQualifiedIdent();
                String u = ident.toUpperCase(Locale.ROOT);
                return switch (u) {
                    case "AND" -> new Token(TokenKind.AND, ident);
                    case "OR" -> new Token(TokenKind.OR, ident);
                    case "NOT" -> new Token(TokenKind.NOT, ident);
                    case "IS" -> new Token(TokenKind.IS, ident);
                    case "LIKE" -> new Token(TokenKind.LIKE, ident);
                    case "IN" -> new Token(TokenKind.IN, ident);
                    case "TRUE", "FALSE" -> new Token(TokenKind.BOOL, u);
                    case "NULL" -> new Token(TokenKind.NULL, u);
                    default -> new Token(TokenKind.IDENT, ident);
                };
            }

            throw new SQLException("Unexpected character in WHERE: '" + c + "'");
        }

        private String readQualifiedIdent() throws SQLException {
            StringBuilder out = new StringBuilder();
            out.append(readSegment());
            while (true) {
                skipWs();
                if (i < s.length() && s.charAt(i) == '.') {
                    i++;
                    out.append('.').append(readSegment());
                } else break;
            }
            return out.toString();
        }

        private String readSegment() throws SQLException {
            skipWs();
            if (i >= s.length()) throw new SQLException("Expected identifier segment");

            char c = s.charAt(i);

            if (c == '"') {
                int j = s.indexOf('"', i + 1);
                if (j < 0) throw new SQLException("Unterminated quoted identifier");
                String seg = s.substring(i + 1, j);
                i = j + 1;
                return seg;
            }
            if (c == '`') {
                int j = s.indexOf('`', i + 1);
                if (j < 0) throw new SQLException("Unterminated quoted identifier");
                String seg = s.substring(i + 1, j);
                i = j + 1;
                return seg;
            }
            if (c == '[') {
                int j = s.indexOf(']', i + 1);
                if (j < 0) throw new SQLException("Unterminated bracket identifier");
                String seg = s.substring(i + 1, j);
                i = j + 1;
                return seg;
            }

            int j = i;
            while (j < s.length()) {
                char x = s.charAt(j);
                if (Character.isLetterOrDigit(x) || x == '_' ) j++;
                else break;
            }
            if (j == i) throw new SQLException("Bad identifier");
            String seg = s.substring(i, j);
            i = j;
            return seg;
        }

        private void skipWs() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        }
    }

    static final class Parser {
        private final Tokenizer tz;
        private final Map<String, Integer> colIndex;

        Parser(Tokenizer tz, String[] colNames) {
            this.tz = tz;
            this.colIndex = buildColIndex(colNames);
        }

        Predicate parseExpr() throws SQLException { return parseOr(); }

        private Predicate parseOr() throws SQLException {
            Predicate left = parseAnd();
            while (tz.accept(TokenKind.OR)) {
                Predicate right = parseAnd();
                Predicate a = left;
                left = row -> a.test(row) || right.test(row);
            }
            return left;
        }

        private Predicate parseAnd() throws SQLException {
            Predicate left = parseUnary();
            while (tz.accept(TokenKind.AND)) {
                Predicate right = parseUnary();
                Predicate a = left;
                left = row -> a.test(row) && right.test(row);
            }
            return left;
        }

        private Predicate parseUnary() throws SQLException {
            if (tz.accept(TokenKind.NOT)) {
                Predicate inner = parseUnary();
                return row -> !inner.test(row);
            }
            if (tz.accept(TokenKind.LPAREN)) {
                Predicate inner = parseExpr();
                tz.expect(TokenKind.RPAREN);
                return inner;
            }
            return parsePredicate();
        }

        private Predicate parsePredicate() throws SQLException {
            Operand lhs = parseOperand();

            if (tz.accept(TokenKind.IS)) {
                boolean not = tz.accept(TokenKind.NOT);
                tz.expect(TokenKind.NULL);
                return row -> {
                    Object v = lhs.get(row);
                    return not == (v != null);
                };
            }

            if (tz.accept(TokenKind.LIKE)) {
                Operand rhs = parseOperand();
                return row -> {
                    Object lv = lhs.get(row);
                    Object rv = rhs.get(row);
                    if (!(lv instanceof String ls) || !(rv instanceof String pat)) return false;
                    return like(ls, pat);
                };
            }

            if (tz.accept(TokenKind.IN)) {
                tz.expect(TokenKind.LPAREN);
                List<Operand> elems = new ArrayList<>();
                elems.add(parseOperand());
                while (tz.accept(TokenKind.COMMA)) elems.add(parseOperand());
                tz.expect(TokenKind.RPAREN);

                return row -> {
                    Object lv = lhs.get(row);
                    for (Operand e : elems) {
                        Object rv = e.get(row);
                        if (Objects.equals(lv, rv)) return true;
                    }
                    return false;
                };
            }

            Token op = tz.next();
            if (op.kind != TokenKind.OP) throw new SQLException("Expected comparison operator, got " + op.kind);

            Operand rhs = parseOperand();
            String opText = op.text;

            return row -> compare(lhs.get(row), rhs.get(row), opText);
        }

        private Operand parseOperand() throws SQLException {
            Token t = tz.peek();
            return switch (t.kind) {
                case IDENT -> {
                    String id = tz.next().text;
                    int idx = resolveColumnIndex(id);
                    yield new ColumnOperand(idx);
                }
                case STRING -> new LiteralOperand(tz.next().text);
                case NUMBER -> new LiteralOperand(parseNumber(tz.next().text));
                case BOOL -> new LiteralOperand(tz.next().text.equalsIgnoreCase("TRUE"));
                case NULL -> { tz.next(); yield new LiteralOperand(null); }
                default -> throw new SQLException("Expected operand, got " + t.kind + " (" + t.text + ")");
            };
        }

        private int resolveColumnIndex(String ident) throws SQLException {
            String base = ident;
            int dot = base.lastIndexOf('.');
            if (dot >= 0) base = base.substring(dot + 1);

            Integer idx = colIndex.get(base.toLowerCase(Locale.ROOT));
            if (idx == null) throw new SQLException("Unknown column in WHERE: " + ident);
            return idx;
        }

        private Map<String, Integer> buildColIndex(String[] names) {
            Map<String, Integer> m = new HashMap<>();
            for (int i = 0; i < names.length; i++) {
                if (names[i] == null) continue;
                m.putIfAbsent(names[i].toLowerCase(Locale.ROOT), i);
            }
            return m;
        }

        private Object parseNumber(String s) {
            String t = s.trim();
            if (t.contains(".") || t.contains("e") || t.contains("E")) {
                try { return Double.parseDouble(t); } catch (Exception ignored) { return t; }
            }
            try { return Long.parseLong(t); } catch (Exception ignored) { return t; }
        }
    }

    interface Operand { Object get(RowAccessor row) throws SQLException; }

    record ColumnOperand(int index) implements Operand {
        @Override public Object get(RowAccessor row) throws SQLException { return row.get(index); }
    }

    record LiteralOperand(Object value) implements Operand {
        @Override public Object get(RowAccessor row) { return value; }
    }

    private static boolean compare(Object l, Object r, String op) {
        if ("=".equals(op)) return Objects.equals(l, r);
        if ("!=".equals(op) || "<>".equals(op)) return !Objects.equals(l, r);

        if (l == null || r == null) return false;

        int c = cmp(l, r);
        return switch (op) {
            case "<" -> c < 0;
            case "<=" -> c <= 0;
            case ">" -> c > 0;
            case ">=" -> c >= 0;
            default -> false;
        };
    }

    private static int cmp(Object l, Object r) {
        if (l instanceof Number ln && r instanceof Number rn) {
            boolean floaty = (ln instanceof Float || ln instanceof Double) || (rn instanceof Float || rn instanceof Double);
            if (floaty) return Double.compare(ln.doubleValue(), rn.doubleValue());
            return Long.compare(ln.longValue(), rn.longValue());
        }
        if (l instanceof String ls && r instanceof String rs) return ls.compareTo(rs);

        if (l.getClass().equals(r.getClass()) && l instanceof Comparable c) {
            @SuppressWarnings("unchecked")
            Comparable<Object> cc = (Comparable<Object>) c;
            return cc.compareTo(r);
        }

        return String.valueOf(l).compareTo(String.valueOf(r));
    }

    private static boolean like(String value, String sqlPattern) {
        StringBuilder re = new StringBuilder();
        re.append("^");
        for (int i = 0; i < sqlPattern.length(); i++) {
            char c = sqlPattern.charAt(i);
            if (c == '%') re.append(".*");
            else if (c == '_') re.append(".");
            else {
                if ("\\.[]{}()*+-?^$|".indexOf(c) >= 0) re.append("\\");
                re.append(c);
            }
        }
        re.append("$");
        return Pattern.compile(re.toString(), Pattern.DOTALL).matcher(value).matches();
    }
}
