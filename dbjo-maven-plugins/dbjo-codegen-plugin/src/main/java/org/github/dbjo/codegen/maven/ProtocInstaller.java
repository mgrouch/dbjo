package org.github.dbjo.codegen.maven;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResult;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public final class ProtocInstaller {

    /**
     * protocExe   = installDir/bin/protoc(.exe)
     * includeDir  = installDir/include
     * version     = protoc version
     * platform    = maven classifier (windows-x86_64, linux-x86_64, osx-aarch_64, ...)
     */
    public record ProtocPaths(Path protocExe, Path includeDir, String version, String platform) {}

    private ProtocInstaller() {}

    /**
     * Ensures protoc is installed under installDir by resolving Maven artifacts:
     *
     *  - com.google.protobuf:protoc:${protocVersion}:exe:${classifier}
     *  - com.google.protobuf:protobuf-java:${protobufJavaVersion}  (extract google/protobuf/*.proto)
     *
     * This will download from your Artifactory if it's configured as a mirror/repo in Maven.
     *
     * Behavior:
     *  - If installDir/bin/protoc(.exe) already exists and is usable, we DO NOT download protoc again.
     *  - If includeDir is missing, we will still (re)extract WKTs from protobuf-java.
     *  - Marker file is advisory only (used for quick match) and is refreshed when we reuse an existing exe.
     */
    public static ProtocPaths ensureInstalledFromMaven(
            Log log,
            Path installDir,
            String protocVersion,
            String protobufJavaVersion,
            RepositorySystem repoSystem,
            RepositorySystemSession repoSession,
            List<RemoteRepository> remoteRepos,
            boolean offline
    ) throws MojoExecutionException {

        Objects.requireNonNull(log, "log");
        Objects.requireNonNull(installDir, "installDir");
        Objects.requireNonNull(protocVersion, "protocVersion");
        Objects.requireNonNull(protobufJavaVersion, "protobufJavaVersion");
        Objects.requireNonNull(repoSystem, "repoSystem");
        Objects.requireNonNull(repoSession, "repoSession");
        Objects.requireNonNull(remoteRepos, "remoteRepos");

        String classifier = detectMavenClassifier();

        Path bin = installDir.resolve("bin");
        Path exe = bin.resolve(isWindows() ? "protoc.exe" : "protoc");
        Path include = installDir.resolve("include");
        Path marker = installDir.resolve(".dbjo-protoc.marker");

        // ---------------------------------------------------------------------
        // Fast path #0: if protoc exe exists and is usable, reuse it.
        // Marker match is NOT required (user may preinstall / overwrite).
        // If include dir is missing we still install includes (protobuf-java).
        // ---------------------------------------------------------------------
        boolean exeUsable = false;
        if (Files.isRegularFile(exe)) {
            try {
                ensureExecutable(exe); // chmod on *nix as needed; no-op on Windows
                exeUsable = isWindows() || Files.isExecutable(exe);
            } catch (Exception e) {
                exeUsable = false;
                log.warn("Existing protoc not usable (" + exe + "): " + e.getMessage());
            }
        }

        if (exeUsable && Files.isDirectory(include)) {
            log.info("Using existing protoc: " + exe);
            // refresh marker best-effort
            try { writeMarker(marker, protocVersion, classifier, protobufJavaVersion); } catch (Exception ignored) {}
            return new ProtocPaths(exe, include, protocVersion, classifier);
        }

        // ---------------------------------------------------------------------
        // Fast path #1: strict match (exe + include + marker matches)
        // ---------------------------------------------------------------------
        if (Files.isRegularFile(exe) && Files.isDirectory(include)
                && markerMatches(marker, protocVersion, classifier, protobufJavaVersion)) {
            log.info("protoc already installed (marker match): " + exe);
            return new ProtocPaths(exe, include, protocVersion, classifier);
        }

        // If offline: we can still proceed if protoc exe exists but include is missing
        // only if include already exists; otherwise we need protobuf-java jar.
        if (offline) {
            if (exeUsable && Files.isDirectory(include)) {
                return new ProtocPaths(exe, include, protocVersion, classifier);
            }
            throw new MojoExecutionException(
                    "Maven is offline and protoc is missing/incomplete.\n" +
                            "Expected protoc exe: " + exe + "\n" +
                            "Expected include dir: " + include + "\n" +
                            "Either run once online or preinstall protoc+includes and set -Dprotoc / -Dprotoc.include."
            );
        }

        try {
            Files.createDirectories(installDir);

            // We always refresh includes (safe and cheap), but do NOT delete bin/exe if it is usable.
            deleteIfExists(include);
            Files.createDirectories(include);

            // 1) Ensure protoc exe (download only if missing/unusable)
            if (!exeUsable) {
                // clean bin only when we need to install protoc
                deleteIfExists(bin);
                Files.createDirectories(bin);

                Artifact protocArt = new org.eclipse.aether.artifact.DefaultArtifact(
                        "com.google.protobuf:protoc:" + protocVersion + ":exe:" + classifier
                );
                Path protocFile = resolveArtifactFile(log, repoSystem, repoSession, remoteRepos, protocArt);

                Files.copy(protocFile, exe, REPLACE_EXISTING);
                ensureExecutable(exe);

                log.info("Installed protoc exe: " + exe);
            } else {
                log.info("Reusing protoc exe: " + exe);
            }

            // 2) Install includes from protobuf-java (WKTs etc.)
            Artifact pbJavaArt = new org.eclipse.aether.artifact.DefaultArtifact(
                    "com.google.protobuf:protobuf-java:" + protobufJavaVersion
            );
            Path pbJavaJar = resolveArtifactFile(log, repoSystem, repoSession, remoteRepos, pbJavaArt);

            extractProtosFromJar(pbJavaJar, include);

            writeMarker(marker, protocVersion, classifier, protobufJavaVersion);

            log.info("protoc ready: " + exe);
            log.info("protoc includes: " + include);
            return new ProtocPaths(exe, include, protocVersion, classifier);

        } catch (Exception e) {
            throw new MojoExecutionException("Failed installing protoc into: " + installDir, e);
        }
    }

    /**
     * Infer protoc version from protobuf-java version.
     * Example: protobuf-java 4.33.2 -> protoc 33.2 ; protobuf-java 3.25.4 -> protoc 25.4
     */
    public static String inferProtocVersionFromProtobufJava(String protobufJavaVersion) {
        if (protobufJavaVersion == null) return null;
        String v = protobufJavaVersion.trim();
        if (v.isEmpty()) return null;

        String[] parts = v.split("\\.");
        if (parts.length < 3) return null;

        String minor = parts[1];
        String patch = parts[2].replaceAll("[^0-9].*$", "");
        if (minor.isBlank() || patch.isBlank()) return null;

        return minor + "." + patch;
    }

    // ----------------- internals -----------------

    private static Path resolveArtifactFile(
            Log log,
            RepositorySystem repoSystem,
            RepositorySystemSession repoSession,
            List<RemoteRepository> remoteRepos,
            Artifact artifact
    ) throws MojoExecutionException {
        try {
            ArtifactRequest req = new ArtifactRequest();
            req.setArtifact(artifact);
            req.setRepositories(remoteRepos);

            log.info("Resolving artifact: " + artifact);
            ArtifactResult res = repoSystem.resolveArtifact(repoSession, req);

            if (res == null || res.getArtifact() == null || res.getArtifact().getFile() == null) {
                throw new MojoExecutionException("Failed to resolve artifact: " + artifact);
            }
            return res.getArtifact().getFile().toPath();
        } catch (Exception e) {
            throw new MojoExecutionException("Artifact resolve failed: " + artifact, e);
        }
    }

    private static void extractProtosFromJar(Path jarFile, Path includeDir) throws IOException {
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(jarFile))) {
            ZipEntry e;
            while ((e = zis.getNextEntry()) != null) {
                String name = e.getName();
                if (e.isDirectory()) {
                    zis.closeEntry();
                    continue;
                }
                if (!name.startsWith("google/protobuf/") || !name.endsWith(".proto")) {
                    zis.closeEntry();
                    continue;
                }

                Path outPath = includeDir.resolve(name).normalize();
                if (!outPath.startsWith(includeDir)) {
                    throw new IOException("Zip traversal attempt: " + name);
                }

                Files.createDirectories(outPath.getParent());
                try (OutputStream os = Files.newOutputStream(outPath,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                    zis.transferTo(os);
                }
                zis.closeEntry();
            }
        }
    }

    private static void ensureExecutable(Path file) throws IOException {
        if (isWindows()) return;
        if (Files.isExecutable(file)) return;

        boolean ok = file.toFile().setExecutable(true);
        if (Files.isExecutable(file)) return;

        try {
            Set<PosixFilePermission> perms = EnumSet.of(
                    PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                    PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
                    PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE
            );
            Files.setPosixFilePermissions(file, perms);
        } catch (UnsupportedOperationException ignored) {}

        if (!Files.isExecutable(file) && !ok) {
            throw new IOException("protoc exists but is not executable: " + file);
        }
    }

    private static boolean markerMatches(Path marker, String protocV, String classifier, String pbJavaV) {
        if (!Files.isRegularFile(marker)) return false;
        try {
            String s = Files.readString(marker, StandardCharsets.UTF_8).trim();
            return s.equals(protocV + "|" + classifier + "|" + pbJavaV);
        } catch (IOException e) {
            return false;
        }
    }

    private static void writeMarker(Path marker, String protocV, String classifier, String pbJavaV) throws IOException {
        Files.writeString(marker, protocV + "|" + classifier + "|" + pbJavaV, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static void deleteIfExists(Path p) throws IOException {
        if (!Files.exists(p)) return;
        if (Files.isDirectory(p)) {
            try (var st = Files.walk(p)) {
                st.sorted(Comparator.reverseOrder()).forEach(x -> {
                    try { Files.deleteIfExists(x); } catch (IOException ignored) {}
                });
            }
        } else {
            Files.deleteIfExists(p);
        }
    }

    /**
     * Maven protoc classifiers:
     *  - windows-x86_64
     *  - linux-x86_64 / linux-aarch_64
     *  - osx-x86_64 / osx-aarch_64
     */
    private static String detectMavenClassifier() throws MojoExecutionException {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);

        boolean arm64 = arch.contains("aarch64") || arch.contains("arm64");
        boolean x64 = arch.contains("x86_64") || arch.contains("amd64");

        if (os.contains("win")) {
            if (!x64) throw new MojoExecutionException("Unsupported Windows arch: " + arch + " (only x86_64 supported)");
            return "windows-x86_64";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            if (arm64) return "osx-aarch_64";
            if (x64) return "osx-x86_64";
            throw new MojoExecutionException("Unsupported macOS arch: " + arch);
        }
        // assume linux
        if (arm64) return "linux-aarch_64";
        if (x64) return "linux-x86_64";

        throw new MojoExecutionException("Unsupported platform: os=" + os + " arch=" + arch);
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }
}
