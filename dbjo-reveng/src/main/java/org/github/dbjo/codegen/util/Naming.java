package org.github.dbjo.codegen.util;

import java.util.Locale;
import java.util.Set;

public final class Naming {

    public static final Set<String> JAVA_KEYWORDS = Set.of(
            "abstract","continue","for","new","switch",
            "assert","default","goto","package","synchronized",
            "boolean","do","if","private","this",
            "break","double","implements","protected","throw",
            "byte","else","import","public","throws",
            "case","enum","instanceof","return","transient",
            "catch","extends","int","short","try",
            "char","final","interface","static","void",
            "class","finally","long","strictfp","volatile",
            "const","float","native","super","while",
            "true","false","null"
    );

    private Naming() {}

    public static String toClassName(String tableName) {
        String camel = toCamelCase(tableName, true);
        if (!camel.isEmpty() && Character.isDigit(camel.charAt(0))) camel = "_" + camel;
        return camel.isEmpty() ? "Table" : camel;
    }

    public static String toFieldName(String columnName) {
        String camel = toCamelCase(columnName, false);
        if (!camel.isEmpty() && Character.isDigit(camel.charAt(0))) camel = "_" + camel;
        return camel.isEmpty() ? "field" : camel;
    }

    public static String toCamelCase(String s, boolean capFirst) {
        String[] parts = s == null ? new String[0] : s.split("[^A-Za-z\\d]+");
        StringBuilder out = new StringBuilder();
        for (String p : parts) {
            if (p.isEmpty()) continue;
            String lower = p.toLowerCase(Locale.ROOT);
            out.append(Character.toUpperCase(lower.charAt(0))).append(lower.substring(1));
        }
        if (!capFirst && out.length() > 0) out.setCharAt(0, Character.toLowerCase(out.charAt(0)));
        return out.toString();
    }

    public static String toLowerSnake(String s) {
        if (s == null || s.isEmpty()) return s;

        // normalize separators
        String norm = s.replaceAll("[^A-Za-z\\d]+", "_");

        StringBuilder sb = new StringBuilder(norm.length() + 8);
        char prev = 0;

        for (int i = 0; i < norm.length(); i++) {
            char ch = norm.charAt(i);

            if (ch == '_') {
                if (sb.length() > 0 && sb.charAt(sb.length() - 1) != '_') sb.append('_');
                prev = ch;
                continue;
            }

            boolean isUpper = Character.isUpperCase(ch);
            boolean prevIsLowerOrDigit = i > 0 && (Character.isLowerCase(prev) || Character.isDigit(prev));
            boolean prevIsUpper = i > 0 && Character.isUpperCase(prev);
            boolean nextIsLower = (i + 1 < norm.length()) && Character.isLowerCase(norm.charAt(i + 1));

            // handle camelCase and acronym boundaries: "CreatedAt" -> "created_at", "HTTPServer" -> "http_server"
            if (i > 0 && isUpper && (prevIsLowerOrDigit || (prevIsUpper && nextIsLower))) {
                if (sb.length() > 0 && sb.charAt(sb.length() - 1) != '_') sb.append('_');
            }

            sb.append(Character.toLowerCase(ch));
            prev = ch;
        }

        String out = sb.toString().replaceAll("^_+|_+$", "").replaceAll("__+", "_");
        return out.isEmpty() ? "field" : out;
    }

    public static String toUpperSnake(String camel) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < camel.length(); i++) {
            char ch = camel.charAt(i);
            if (Character.isUpperCase(ch) && i > 0) sb.append('_');
            sb.append(Character.toUpperCase(ch));
        }
        return sanitizeJavaIdentifier(sb.toString());
    }

    public static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    public static String sanitizeJavaIdentifier(String name) {
        if (name == null || name.isEmpty()) return name;
        return switch (name) {
            case "class", "public", "private", "protected", "static", "final", "void",
                    "int", "long", "float", "double", "boolean", "byte", "short", "char",
                    "return", "package", "import", "new", "null", "true", "false",
                    "this", "super", "interface", "enum", "extends", "implements",
                    "switch", "case", "default", "break", "continue", "for", "while", "do",
                    "if", "else", "try", "catch", "finally", "throw", "throws", "instanceof" -> name + "_";
            default -> name;
        };
    }

    public static String sanitizeProtoIdentifier(String name) {
        if (name == null || name.isBlank()) return "field";
        String n = name.replaceAll("\\W", "_");
        if (Character.isDigit(n.charAt(0))) n = "_" + n;
        return switch (n) {
            case "package", "syntax", "import", "message", "enum", "service", "rpc",
                    "option", "returns", "reserved" -> n + "_";
            default -> n;
        };
    }

    public static String toUpperConst(String s) {
        if (s == null || s.isBlank()) return "X";
        String n = s.replaceAll("[^A-Za-z\\d]+", "_").toUpperCase(Locale.ROOT);
        if (!n.isEmpty() && Character.isDigit(n.charAt(0))) n = "_" + n;
        return sanitizeJavaIdentifier(n);
    }

    public static String toLowerCamel(String s) {
        String c = toCamelCase(s, false);
        return c.isEmpty() ? "x" : c;
    }
}
