package org.github.dbjo;

import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public final class ServerApp {

  private static final String USER = "SA";
  private static final String PASS = "";

  public static void main(String[] args) throws Exception {
    Server server = new Server();
    server.setProperties(new HsqlProperties());
    server.setSilent(true);
    server.setTrace(false);

    // In-memory DB hosted by server
    server.setDatabaseName(0, "dbjo");
    server.setDatabasePath(0, "mem:dbjo");
    server.setPort(9001);

    server.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try { server.stop(); } catch (Exception ignored) {}
    }));

    // Connect through TCP to initialize schema
    String url = "jdbc:hsqldb:hsql://localhost:9001/dbjo";
    try (Connection conn = DriverManager.getConnection(url, USER, PASS)) {
      SqlRunner.runClasspathScript(conn, "schema.sql");

      try (PreparedStatement ps = conn.prepareStatement(
              "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
                      "WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_TYPE = 'BASE TABLE' " +
                      "ORDER BY TABLE_NAME")) {
        try (ResultSet rs = ps.executeQuery()) {
          System.out.println("Tables in PUBLIC schema:");
          while (rs.next()) System.out.println(" - " + rs.getString(1));
        }
      }
    }

    System.out.println();
    System.out.println("HSQLDB Server running.");
    System.out.println("Connect using: jdbc:hsqldb:hsql://localhost:9001/dbjo");
    System.out.println("Press ENTER to stop...");
    System.in.read();

    server.stop();
  }
}
