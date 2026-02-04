package org.github.dbjo.hsql.maven;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.*;

import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;

import java.sql.Connection;

@Mojo(
        name = "start",
        defaultPhase = LifecyclePhase.NONE,
        threadSafe = true
)
public final class StartHsqldbMojo extends AbstractMojo {

    @Parameter(property = "dbjo.hsqldb.dbName", defaultValue = "dbjo")
    private String dbName;

    @Parameter(property = "dbjo.hsqldb.dbPath", defaultValue = "mem:dbjo")
    private String dbPath;

    @Parameter(property = "dbjo.hsqldb.port", defaultValue = "9001")
    private int port;

    @Parameter(property = "dbjo.hsqldb.user", defaultValue = "SA")
    private String user;

    @Parameter(property = "dbjo.hsqldb.pass")
    private String pass;

    /** JDBC URL used for init + stop. If empty, derived from port/dbName. */
    @Parameter(property = "dbjo.hsqldb.jdbcUrl")
    private String jdbcUrl;

    /** Optional script to run after server starts (schema init). */
    @Parameter(property = "dbjo.hsqldb.initScript")
    private String initScript;

    /** If true, waits forever; if false, returns immediately (server will die with Maven JVM). */
    @Parameter(property = "dbjo.hsqldb.block", defaultValue = "true")
    private boolean block;

    /** Poll wait until JDBC connect succeeds (ms). */
    @Parameter(property = "dbjo.hsqldb.waitMs", defaultValue = "15000")
    private long waitMs;

    /** Allowed Java class names for stored procedures / functions. */
    @Parameter(property = "dbjo.hsqldb.methodClassNames",
            defaultValue = "org.github.dbjo.meta.features.PartitionId")
    private String methodClassNames;

    @Override
    public void execute() throws MojoExecutionException {
        try {
            String url = (jdbcUrl != null && !jdbcUrl.isBlank())
                    ? jdbcUrl
                    : "jdbc:hsqldb:hsql://localhost:" + port + "/" + dbName;

            getLog().info("Starting HSQLDB Server");
            getLog().info("  dbName   = " + dbName);
            String resolvedDbPath = dbPath;
            if (methodClassNames != null && !methodClassNames.isBlank()
                    && !dbPath.contains("hsqldb.method_class_names")) {
                resolvedDbPath = dbPath + ";hsqldb.method_class_names=" + methodClassNames;
            }

            getLog().info("  dbPath   = " + resolvedDbPath);
            getLog().info("  port     = " + port);
            getLog().info("  jdbcUrl  = " + url);
            getLog().info("  block    = " + block);
            getLog().info("  methods  = " + (methodClassNames == null ? "" : methodClassNames));

            HsqlProperties properties = new HsqlProperties();

            Server server = new Server();
            server.setProperties(properties);
            server.setSilent(true);
            server.setTrace(false);

            server.setDatabaseName(0, dbName);
            server.setDatabasePath(0, resolvedDbPath);
            server.setPort(port);

            server.start();

            // Ensure shutdown on Maven JVM exit (Ctrl+C)
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try { server.stop(); } catch (Exception ignored) {}
            }));

            waitForJdbc(url, user, pass, waitMs);

            if (initScript != null && !initScript.isBlank()) {
                getLog().info("Running init script: " + initScript);
                try (Connection c = HsqlSupport.connect(url, user, pass)) {
                    HsqlSupport.runSqlScript(c, initScript);
                }
            }

            getLog().info("HSQLDB Server is running.");

            if (block) {
                getLog().info("Blocking. Use another terminal to run: mvn dbjo-hsqldb:stop");
                // Keep the Mojo alive
                // (We rely on stop goal OR Ctrl+C to exit)
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to start HSQLDB", e);
        }
    }

    private void waitForJdbc(String url, String user, String pass, long waitMs) throws Exception {
        long deadline = System.currentTimeMillis() + Math.max(1000L, waitMs);
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try (Connection c = HsqlSupport.connect(url, user, pass)) {
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(200L);
            }
        }
        throw new MojoExecutionException("Timed out waiting for JDBC connect to " + url, last);
    }
}
