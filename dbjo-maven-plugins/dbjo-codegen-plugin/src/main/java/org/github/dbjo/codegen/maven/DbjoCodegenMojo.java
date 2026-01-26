package org.github.dbjo.codegen.maven;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs org.github.dbjo.codegen.DbjoCodegen during Maven build.
 *
 * Goal: dbjo:generate
 * @noinspection unused
 */
@Mojo(
        name = "generate",
        defaultPhase = LifecyclePhase.GENERATE_SOURCES,
        threadSafe = true
)
public final class DbjoCodegenMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

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
    @Parameter(property = "dbjo.codegen.pass")
    private String pass;

    /** Overwrite generated files. */
    @Parameter(property = "dbjo.codegen.overwrite", defaultValue = "false")
    private boolean overwrite;

    /**
     * Java output directory for generated sources.
     * Generator should honor --codegenOutJava=...
     */
    @Parameter(
            property = "dbjo.codegen.codegenOutJava",
            defaultValue = "${project.build.directory}/generated-sources/dbjo"
    )
    private File codegenOutJava;

    /**
     * Protobuf Java output directory.
     * Generator should honor --protoOutJava=...
     */
    @Parameter(
            property = "dbjo.codegen.protoOutJava",
            defaultValue = "${project.build.directory}/generated-sources/dbjo"
    )
    private File protoOutJava;

    /** Add generated dirs as compile source roots automatically. */
    @Parameter(property = "dbjo.codegen.addCompileRoots", defaultValue = "true")
    private boolean addCompileRoots;

    /**
     * Extra raw arguments passed to generator.
     * Example:
     * <args>
     *   <arg>--beanPkg=org.github.dbjo.rdb.demo.generated.entity</arg>
     *   <arg>--dbMetaPkg=org.github.dbjo.rdb.demo.generated.dbmeta</arg>
     * </args>
     * @noinspection MismatchedQueryAndUpdateOfCollection
     */
    @Parameter
    private List<String> args;

    @Parameter(property = "dbjo.codegen.protobufJavaVersion", defaultValue = "${protobuf.version}")
    private String protobufJavaVersion;

    @Parameter(property = "dbjo.codegen.protocVersion")
    private String protocVersion; // optional override (e.g. "33.2")

    @Parameter(property = "dbjo.codegen.protocInstallDir", defaultValue = "${project.build.directory}/tools/protoc")
    private File protocInstallDir;

    @Parameter(property = "dbjo.codegen.downloadProtoc", defaultValue = "true")
    private boolean downloadProtoc;

    @Parameter(
            property = "dbjo.codegen.protocBaseUrl",
            defaultValue = "https://github.com/protocolbuffers/protobuf/releases/download"
    )
    private String protocBaseUrl;

    @Parameter(defaultValue = "${settings.offline}", readonly = true)
    private boolean offline;

    // enum overrides integration (passed through to generator CLI)

    @Parameter(
            property = "dbjo.codegen.enumOverridesFile",
            defaultValue = "${project.basedir}/dbjo-enum-overrides.properties"
    )
    private File enumOverridesFile;

    @Parameter(
            property = "dbjo.codegen.strictUnique",
            defaultValue = "true"
    )
    private boolean strictUnique;

    @Override
    public void execute() throws MojoExecutionException {
        if (skip) {
            getLog().info("dbjo-codegen: skipped");
            return;
        }

        List<String> argv = new ArrayList<>();

        // Common args
        argv.add("--run=" + nz(run, "all"));
        argv.add("--driver=" + nz(driver, ""));
        if (url != null && !url.isBlank()) argv.add("--url=" + url.trim());
        argv.add("--user=" + nz(user, ""));
        argv.add("--pass=" + nz(pass, ""));
        argv.add("--overwrite=" + overwrite);

        argv.add("--codegenOutJava=" + mustFile(codegenOutJava, "codegenOutJava").getAbsolutePath());
        argv.add("--protoOutJava=" + mustFile(protoOutJava, "protoOutJava").getAbsolutePath());

        // pass enum override options to generator (it must support these flags)
        if (enumOverridesFile != null) {
            argv.add("--enumOverridesFile=" + enumOverridesFile.getAbsolutePath());
        }
        argv.add("--strictUnique=" + strictUnique);

        // Extra args passthrough
        if (args != null) {
            for (String a : args) {
                if (a == null) continue;
                String s = a.trim();
                if (!s.isEmpty()) argv.add(s);
            }
        }

        getLog().info("dbjo-codegen: invoking generator");
        getLog().debug("dbjo-codegen args: " + argv);

        // Ensure output dirs exist
        mkdirs(codegenOutJava);
        mkdirs(protoOutJava);

        // Respect explicit -Dprotoc / -Dprotoc.include if user provided them
        String sysProtoc = System.getProperty("protoc");
        String sysInclude = System.getProperty("protoc.include");

        if ((sysProtoc == null || sysProtoc.isBlank() || sysInclude == null || sysInclude.isBlank()) && downloadProtoc) {
            String effVer = (protocVersion != null && !protocVersion.isBlank())
                    ? protocVersion.trim()
                    : ProtocInstaller.inferProtocVersionFromProtobufJava(protobufJavaVersion);

            if (effVer == null || effVer.isBlank()) {
                throw new MojoExecutionException(
                        "Cannot infer protoc version. Set <protocVersion>33.2</protocVersion> " +
                                "or provide <protobufJavaVersion>4.33.2</protobufJavaVersion>."
                );
            }

            var paths = ProtocInstaller.ensureInstalled(
                    getLog(),
                    mustFile(protocInstallDir, "protocInstallDir").toPath(),
                    effVer,
                    nz(protocBaseUrl, "https://github.com/protocolbuffers/protobuf/releases/download"),
                    offline
            );

            // Make existing codegen pick it up via System properties
            System.setProperty("protoc", paths.protocExe().toAbsolutePath().toString());
            System.setProperty("protoc.include", paths.includeDir().toAbsolutePath().toString());

            getLog().info("Using protoc=" + System.getProperty("protoc"));
            getLog().info("Using protoc.include=" + System.getProperty("protoc.include"));
        }

        // Invoke org.github.dbjo.codegen.DbjoCodegen.main(String[])
        invokeGenerator(argv.toArray(String[]::new));

        if (addCompileRoots) {
            addCompileRootIfExists(codegenOutJava);
            addCompileRootIfExists(protoOutJava);
        }
    }

    private static String nz(String s, String dflt) {
        return (s == null) ? dflt : s;
    }

    private static File mustFile(File f, String name) throws MojoExecutionException {
        if (f == null) throw new MojoExecutionException("Missing required configuration: " + name);
        return f;
    }

    private void mkdirs(File dir) throws MojoExecutionException {
        if (dir == null) return;
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
}
