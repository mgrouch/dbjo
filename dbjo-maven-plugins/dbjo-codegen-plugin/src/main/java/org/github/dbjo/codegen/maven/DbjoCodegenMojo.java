package org.github.dbjo.codegen.maven;

import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.*;
import org.apache.maven.project.MavenProject;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.repository.RemoteRepository;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs org.github.dbjo.codegen.DbjoCodegen (your generator) during Maven build.
 *
 * Goal: dbjo-codegen:generate  (your plugin goalPrefix decides the prefix)
 */
@Mojo(
        name = "generate",
        defaultPhase = LifecyclePhase.GENERATE_SOURCES,
        threadSafe = true,
        requiresProject = true
)
public final class DbjoCodegenMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    @Parameter(defaultValue = "${session}", readonly = true, required = true)
    private MavenSession session;

    // Maven Resolver injection (downloads from Artifactory via Maven repos/mirrors)
    @Component
    private RepositorySystem repoSystem;

    @Parameter(defaultValue = "${repositorySystemSession}", readonly = true, required = true)
    private RepositorySystemSession repoSession;

    // Plugin repos are the right choice for plugin downloads; they honor mirrors/settings.xml
    @Parameter(defaultValue = "${project.remotePluginRepositories}", readonly = true, required = true)
    private List<RemoteRepository> remoteRepos;

    /** Skip code generation. */
    @Parameter(property = "dbjo.codegen.skip", defaultValue = "false")
    private boolean skip;

    /**
     * Run mode (passed to generator as --run=...).
     * Examples: all, proto, entity, rdb, dao, mapper, schema, dbmeta
     */
    @Parameter(property = "dbjo.codegen.run", defaultValue = "all")
    private String run;

    /** JDBC driver class name. */
    @Parameter(property = "dbjo.codegen.driver", defaultValue = "org.hsqldb.jdbc.JDBCDriver")
    private String driver;

    /** JDBC url. */
    @Parameter(property = "dbjo.codegen.url")
    private String url;

    /** JDBC user. */
    @Parameter(property = "dbjo.codegen.user", defaultValue = "SA")
    private String user;

    /** JDBC password. */
    @Parameter(property = "dbjo.codegen.pass", defaultValue = "")
    private String pass;

    /** Overwrite generated files. */
    @Parameter(property = "dbjo.codegen.overwrite", defaultValue = "false")
    private boolean overwrite;

    /** Java output directory for generated sources. */
    @Parameter(property = "dbjo.codegen.codegenOutJava",
            defaultValue = "${project.build.directory}/generated-sources/dbjo")
    private File codegenOutJava;

    /** Protobuf Java output directory (if you run protoc). */
    @Parameter(property = "dbjo.codegen.protoOutJava",
            defaultValue = "${project.build.directory}/generated-sources/dbjo")
    private File protoOutJava;

    /** Add generated dirs as compile source roots automatically. */
    @Parameter(property = "dbjo.codegen.addCompileRoots", defaultValue = "true")
    private boolean addCompileRoots;

    /** Extra raw arguments passed to generator. */
    @Parameter
    private List<String> args;

    /**
     * protobuf-java version (used for WKT proto includes and to infer protoc if protocVersion missing).
     * You can pass it explicitly from the consuming POM: <protobufJavaVersion>${protobuf.version}</protobufJavaVersion>
     */
    @Parameter(property = "dbjo.codegen.protobufJavaVersion", defaultValue = "")
    private String protobufJavaVersion;

    /** Optional override (e.g. "33.2"). If empty, inferred from protobufJavaVersion. */
    @Parameter(property = "dbjo.codegen.protocVersion", defaultValue = "")
    private String protocVersion;

    @Parameter(property = "dbjo.codegen.protocInstallDir", defaultValue = "${project.build.directory}/tools/protoc")
    private File protocInstallDir;

    @Parameter(property = "dbjo.codegen.downloadProtoc", defaultValue = "true")
    private boolean downloadProtoc;

    @Override
    public void execute() throws MojoExecutionException {
        if (skip) {
            getLog().info("dbjo-codegen: skipped");
            return;
        }

        List<String> argv = new ArrayList<>();
        argv.add("--run=" + run);
        argv.add("--driver=" + driver);

        if (url != null && !url.isBlank()) argv.add("--url=" + url);

        String user0 = (user == null) ? "" : user;
        String pass0 = (pass == null) ? "" : pass;
        argv.add("--user=" + user0);
        argv.add("--pass=" + pass0);
        argv.add("--overwrite=" + overwrite);

        argv.add("--codegenOutJava=" + codegenOutJava.getAbsolutePath());
        argv.add("--protoOutJava=" + protoOutJava.getAbsolutePath());

        if (args != null) {
            for (String a : args) {
                if (a == null) continue;
                String s = a.trim();
                if (!s.isEmpty()) argv.add(s);
            }
        }

        getLog().info("dbjo-codegen: invoking generator");
        getLog().debug("dbjo-codegen args: " + argv);

        mkdirs(codegenOutJava);
        mkdirs(protoOutJava);

        // Respect explicit -Dprotoc / -Dprotoc.include if already set
        String sysProtoc = System.getProperty("protoc");
        String sysInclude = System.getProperty("protoc.include");

        boolean offline = session.getRequest() != null && session.getRequest().isOffline();

        if ((isBlank(sysProtoc) || isBlank(sysInclude)) && downloadProtoc) {
            String pbJavaVer = protobufJavaVersion;
            if (isBlank(pbJavaVer)) {
                // Try Maven property "protobuf.version" if caller used it
                String prop = project.getProperties().getProperty("protobuf.version");
                if (!isBlank(prop)) pbJavaVer = prop.trim();
            }
            if (isBlank(pbJavaVer)) {
                throw new MojoExecutionException(
                        "protobufJavaVersion is not set.\n" +
                                "Set <protobufJavaVersion>${protobuf.version}</protobufJavaVersion> in plugin config " +
                                "or define -Ddbjo.codegen.protobufJavaVersion=3.25.3"
                );
            }

            String effProtocVer = !isBlank(protocVersion)
                    ? protocVersion.trim()
                    : ProtocInstaller.inferProtocVersionFromProtobufJava(pbJavaVer);

            if (isBlank(effProtocVer)) {
                throw new MojoExecutionException(
                        "Cannot infer protoc version from protobufJavaVersion=" + pbJavaVer + ".\n" +
                                "Set <protocVersion>25.3</protocVersion> (or similar)."
                );
            }

            var paths = ProtocInstaller.ensureInstalledFromMaven(
                    getLog(),
                    protocInstallDir.toPath(),
                    effProtocVer,
                    pbJavaVer,
                    repoSystem,
                    repoSession,
                    remoteRepos,
                    offline
            );

            System.setProperty("protoc", paths.protocExe().toAbsolutePath().toString());
            System.setProperty("protoc.include", paths.includeDir().toAbsolutePath().toString());

            getLog().info("Using protoc=" + System.getProperty("protoc"));
            getLog().info("Using protoc.include=" + System.getProperty("protoc.include"));
        } else {
            getLog().info("Using protoc from system properties:");
            getLog().info("  protoc=" + sysProtoc);
            getLog().info("  protoc.include=" + sysInclude);
        }

        invokeGenerator(argv.toArray(String[]::new));

        if (addCompileRoots) {
            addCompileRootIfExists(codegenOutJava);
            addCompileRootIfExists(protoOutJava);
        }
    }

    private void mkdirs(File dir) throws MojoExecutionException {
        try {
            Files.createDirectories(dir.toPath());
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to create dir: " + dir, e);
        }
    }

    private void addCompileRootIfExists(File dir) {
        if (dir == null) return;
        if (!dir.exists() || !dir.isDirectory()) return;
        String path = dir.getAbsolutePath();
        getLog().info("dbjo-codegen: add compile source root: " + path);
        project.addCompileSourceRoot(path);
    }

    private void invokeGenerator(String[] argv) throws MojoExecutionException {
        try {
            Class<?> cls = Class.forName("org.github.dbjo.codegen.DbjoCodegen");
            Method main = cls.getMethod("main", String[].class);
            main.invoke(null, (Object) argv);
        } catch (Exception e) {
            throw new MojoExecutionException("dbjo-codegen failed", e);
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
