package org.github.dbjo.hsql.maven;

import org.apache.maven.plugin.MojoExecutionException;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Objects;

final class HsqlSupport {
    private HsqlSupport() {}

    static Connection connect(String jdbcUrl, String user, String pass) throws Exception {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        Class.forName("org.hsqldb.jdbc.JDBCDriver");
        return DriverManager.getConnection(jdbcUrl, user, pass);
    }

    /**
     * Runs SQL script from either:
     *  - classpath resource (e.g. "schema.sql"), OR
     *  - file: URI ("file:/.../schema.sql"), OR
     *  - absolute/relative file path
     *
     * Script is split on semicolons; lines starting with -- are treated as comments.
     */
    static void runSqlScript(Connection conn, String scriptLocation) throws Exception {
        if (scriptLocation == null || scriptLocation.isBlank()) return;

        String sql = loadScriptText(scriptLocation);
        if (sql.isBlank()) return;

        String[] stmts = splitStatements(stripComments(sql));
        try (Statement st = conn.createStatement()) {
            for (String s : stmts) {
                String trimmed = s.trim();
                if (trimmed.isBlank()) continue;
                st.execute(trimmed);
            }
        }
    }

    private static String stripComments(String sql) {
        StringBuilder out = new StringBuilder(sql.length());
        String[] lines = sql.split("\\R");
        for (String line : lines) {
            String t = line.trim();
            if (t.startsWith("--")) continue;
            out.append(line).append('\n');
        }
        return out.toString();
    }

    private static String[] splitStatements(String sql) {
        StringBuilder current = new StringBuilder(sql.length());
        java.util.List<String> statements = new java.util.ArrayList<>();
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
        if (index < 0 || index + len > sql.length()) return false;
        if (!sql.regionMatches(true, index, keyword, 0, len)) return false;
        if (index > 0 && Character.isLetterOrDigit(sql.charAt(index - 1))) return false;
        if (index + len < sql.length() && Character.isLetterOrDigit(sql.charAt(index + len))) return false;
        return true;
    }

    private static int skipWhitespace(String sql, int index) {
        int i = index;
        while (i < sql.length() && Character.isWhitespace(sql.charAt(i))) {
            i++;
        }
        return i;
    }

    private static String loadScriptText(String loc) throws Exception {
        if (loc.startsWith("classpath:")) {
            return loadClasspath(loc.substring("classpath:".length()));
        }
        if (loc.startsWith("file:")) {
            return java.nio.file.Files.readString(java.nio.file.Path.of(java.net.URI.create(loc)), StandardCharsets.UTF_8);
        }
        // treat as file path if it exists, else classpath
        java.nio.file.Path p = java.nio.file.Path.of(loc);
        if (java.nio.file.Files.exists(p)) {
            return java.nio.file.Files.readString(p, StandardCharsets.UTF_8);
        }
        return loadClasspath(loc);
    }

    private static String loadClasspath(String resource) throws MojoExecutionException {
        String r = resource.startsWith("/") ? resource.substring(1) : resource;
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(r);
        if (in == null) throw new MojoExecutionException("SQL script not found on classpath: " + resource);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder(16_384);
            String line;
            while ((line = br.readLine()) != null) sb.append(line).append('\n');
            return sb.toString();
        } catch (Exception e) {
            throw new MojoExecutionException("Failed reading classpath script: " + resource, e);
        }
    }
}
