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

        // Very simple splitter: works for typical schema.sql without semicolons in strings.
        String[] stmts = sql.split(";");
        try (Statement st = conn.createStatement()) {
            for (String raw : stmts) {
                String s = stripComments(raw).trim();
                if (s.isBlank()) continue;
                st.execute(s);
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
