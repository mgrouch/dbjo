package org.github.dbjo.codegen.util;

import java.util.Locale;

public final class Naming {
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
        String[] parts = s == null ? new String[0] : s.split("[^A-Za-z0-9]+");
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
        String camel = toCamelCase(s, true);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < camel.length(); i++) {
            char ch = camel.charAt(i);
            if (Character.isUpperCase(ch) && i > 0) sb.append('_');
            sb.append(Character.toLowerCase(ch));
        }
        return sb.toString().replaceAll("[^a-z0-9_]+", "_");
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
        String n = name.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(n.charAt(0))) n = "_" + n;
        return switch (n) {
            case "package", "syntax", "import", "message", "enum", "service", "rpc",
                    "option", "returns", "reserved" -> n + "_";
            default -> n;
        };
    }

    public static String toUpperConst(String s) {
        if (s == null || s.isBlank()) return "X";
        String n = s.replaceAll("[^A-Za-z0-9]+", "_").toUpperCase(Locale.ROOT);
        if (!n.isEmpty() && Character.isDigit(n.charAt(0))) n = "_" + n;
        return sanitizeJavaIdentifier(n);
    }

    public static String toLowerCamel(String s) {
        String c = toCamelCase(s, false);
        return c.isEmpty() ? "x" : c;
    }
}
