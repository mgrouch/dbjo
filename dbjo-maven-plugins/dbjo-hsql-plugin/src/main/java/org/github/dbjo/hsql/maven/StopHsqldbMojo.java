package org.github.dbjo.hsql.maven;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.*;

import java.sql.Connection;
import java.sql.Statement;

@Mojo(
        name = "stop",
        defaultPhase = LifecyclePhase.NONE,
        threadSafe = true
)
public final class StopHsqldbMojo extends AbstractMojo {

    @Parameter(property = "dbjo.hsqldb.port", defaultValue = "9001")
    private int port;

    @Parameter(property = "dbjo.hsqldb.dbName", defaultValue = "dbjo")
    private String dbName;

    @Parameter(property = "dbjo.hsqldb.user", defaultValue = "SA")
    private String user;

    @Parameter(property = "dbjo.hsqldb.pass")
    private String pass;

    /** If empty, derived from port/dbName. */
    @Parameter(property = "dbjo.hsqldb.jdbcUrl")
    private String jdbcUrl;

    @Override
    public void execute() throws MojoExecutionException {
        String url = (jdbcUrl != null && !jdbcUrl.isBlank())
                ? jdbcUrl
                : "jdbc:hsqldb:hsql://localhost:" + port + "/" + dbName;

        getLog().info("Stopping HSQLDB via JDBC SHUTDOWN");
        getLog().info("  jdbcUrl = " + url);

        try (Connection c = HsqlSupport.connect(url, user, pass);
             Statement st = c.createStatement()) {

            // SHUTDOWN will stop the server (for in-mem this is fine)
            st.execute("SHUTDOWN");
            getLog().info("Shutdown command sent.");
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to stop HSQLDB at " + url, e);
        }
    }
}
