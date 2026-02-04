package org.github.dbjo.app;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

public final class HsqlScriptRunner {
    private HsqlScriptRunner() {}

    public static void runScripts(DataSource dataSource, List<String> scriptLocations) throws Exception {
        if (dataSource == null || scriptLocations == null || scriptLocations.isEmpty()) {
            return;
        }
        try (Connection conn = dataSource.getConnection()) {
            for (String location : scriptLocations) {
                runSqlScript(conn, location);
            }
        }
    }

    private static void runSqlScript(Connection conn, String scriptLocation) throws Exception {
        if (scriptLocation == null || scriptLocation.isBlank()) {
            return;
        }

        String sql = loadScriptText(scriptLocation);
        if (sql.isBlank()) {
            return;
        }

        String[] statements = splitStatements(stripComments(sql));
        try (Statement st = conn.createStatement()) {
            for (String statement : statements) {
                String trimmed = statement.trim();
                if (trimmed.isBlank()) {
                    continue;
                }
                st.execute(trimmed);
            }
        }
    }

    private static String stripComments(String sql) {
        StringBuilder out = new StringBuilder(sql.length());
        String[] lines = sql.split("\\R");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.startsWith("--")) {
                continue;
            }
            out.append(line).append('\n');
        }
        return out.toString();
    }

    private static String[] splitStatements(String sql) {
        StringBuilder current = new StringBuilder(sql.length());
        List<String> statements = new ArrayList<>();
        boolean inString = false;
        int depth = 0;

        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            current.append(ch);

            if (ch == '\'') {
                inString = !inString;
                continue;
            }

            if (inString) {
                continue;
            }

            if (ch == ';' && depth == 0) {
                statements.add(current.substring(0, current.length() - 1));
                current.setLength(0);
                continue;
            }

            if (isKeywordAt(sql, i, "BEGIN")) {
                int next = skipWhitespace(sql, i + "BEGIN".length());
                if (isKeywordAt(sql, next, "ATOMIC")) {
                    depth++;
                }
            } else if (isKeywordAt(sql, i, "END") && depth > 0) {
                int next = skipWhitespace(sql, i + "END".length());
                if (!isKeywordAt(sql, next, "IF")
                        && !isKeywordAt(sql, next, "WHILE")
                        && !isKeywordAt(sql, next, "LOOP")
                        && !isKeywordAt(sql, next, "CASE")) {
                    depth--;
                }
            }
        }

        if (current.length() > 0) {
            statements.add(current.toString());
        }

        return statements.toArray(new String[0]);
    }

    private static boolean isKeywordAt(String sql, int index, String keyword) {
        int len = keyword.length();
        if (index < 0 || index + len > sql.length()) {
            return false;
        }
        if (!sql.regionMatches(true, index, keyword, 0, len)) {
            return false;
        }
        if (index > 0 && Character.isLetterOrDigit(sql.charAt(index - 1))) {
            return false;
        }
        if (index + len < sql.length() && Character.isLetterOrDigit(sql.charAt(index + len))) {
            return false;
        }
        return true;
    }

    private static int skipWhitespace(String sql, int index) {
        int i = index;
        while (i < sql.length() && Character.isWhitespace(sql.charAt(i))) {
            i++;
        }
        return i;
    }

    private static String loadScriptText(String location) throws Exception {
        if (location.startsWith("classpath:")) {
            return loadClasspath(location.substring("classpath:".length()));
        }
        if (location.startsWith("file:")) {
            return java.nio.file.Files.readString(java.nio.file.Path.of(java.net.URI.create(location)), StandardCharsets.UTF_8);
        }
        java.nio.file.Path path = java.nio.file.Path.of(location);
        if (java.nio.file.Files.exists(path)) {
            return java.nio.file.Files.readString(path, StandardCharsets.UTF_8);
        }
        return loadClasspath(location);
    }

    private static String loadClasspath(String resource) throws Exception {
        String normalized = resource.startsWith("/") ? resource.substring(1) : resource;
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(normalized);
        if (in == null) {
            throw new IllegalArgumentException("SQL script not found on classpath: " + resource);
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder(16_384);
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
            return sb.toString();
        }
    }
}
