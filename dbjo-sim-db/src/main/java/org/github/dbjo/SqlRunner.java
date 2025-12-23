package org.github.dbjo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Loads a classpath SQL script and executes its statements in order.
 * Assumptions:
 *  - Statements are separated by ';'
 *  - Lines starting with '--' are comments
 *  - No stored procedures / triggers using internal ';' delimiters
 */
public final class SqlRunner {
  private SqlRunner() {}

  public static void runClasspathScript(Connection conn, String resourcePath) throws IOException, SQLException {
    try (InputStream is = SqlRunner.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IOException("SQL resource not found on classpath: " + resourcePath);
      }
      StringBuilder sb = new StringBuilder();
      try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        String line;
        while ((line = br.readLine()) != null) {
          String trimmed = line.trim();
          if (trimmed.startsWith("--") || trimmed.isEmpty()) {
            continue;
          }
          sb.append(line).append('\n');
        }
      }

      String script = sb.toString();
      // Split on semicolons, execute each non-empty statement
      String[] statements = script.split(";");
      try (Statement st = conn.createStatement()) {
        for (String raw : statements) {
          String sql = raw.trim();
          if (sql.isEmpty()) continue;
          st.execute(sql);
        }
      }
    }
  }
}
