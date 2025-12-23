package org.github.dbjo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Minimal demo:
 *  - creates an embedded in-memory HSQLDB (lives for the JVM lifetime)
 *  - executes src/main/resources/schema.sql to create 3 tables
 *  - prints the created tables
 */
public final class App {

  // In-memory DB named "dbjo". "shutdown=true" ensures clean shutdown on close.
  private static final String JDBC_URL = "jdbc:hsqldb:mem:dbjo;shutdown=true";
  private static final String USER = "SA";
  private static final String PASS = "";

  public static void main(String[] args) throws Exception {
    // Connecting starts the embedded in-memory database.
    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASS)) {
      conn.setAutoCommit(true);

      // Create tables via SQL DDL script:
      SqlRunner.runClasspathScript(conn, "schema.sql");

      // Verify by listing user tables:
      try (PreparedStatement ps = conn.prepareStatement(
          "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
          "WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_TYPE = 'BASE TABLE' " +
          "ORDER BY TABLE_NAME")) {
        try (ResultSet rs = ps.executeQuery()) {
          System.out.println("Tables in PUBLIC schema:");
          while (rs.next()) {
            System.out.println(" - " + rs.getString(1));
          }
        }
      }
    }
  }
}
