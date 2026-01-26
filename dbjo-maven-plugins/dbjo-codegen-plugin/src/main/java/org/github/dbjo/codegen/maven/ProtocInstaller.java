package org.github.dbjo.codegen.maven;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public final class ProtocInstaller {

    public record ProtocPaths(Path protocExe, Path includeDir, String version, String platform) {}

    private ProtocInstaller() {}

    /**
     * Ensures protoc is installed under installDir and returns resolved executable + include paths.
     *
     * Layout after unzip:
     *   installDir/
     *     bin/protoc(.exe)
     *     include/google/protobuf/*.proto
     */
    public static ProtocPaths ensureInstalled(
            Log log,
            Path installDir,
            String protocVersion,
            String baseDownloadUrl,   // e.g. https://github.com/protocolbuffers/protobuf/releases/download
            boolean offline
    ) throws MojoExecutionException {

        Objects.requireNonNull(log, "log");
        Objects.requireNonNull(installDir, "installDir");
        Objects.requireNonNull(protocVersion, "protocVersion");
        Objects.requireNonNull(baseDownloadUrl, "baseDownloadUrl");

        String platform = detectPlatform();
        String fileName = "protoc-" + protocVersion + "-" + platform + ".zip";
        URI url = URI.create(trimTrailingSlash(baseDownloadUrl) + "/v" + protocVersion + "/" + fileName);

        Path bin = installDir.resolve("bin");
        Path exe = bin.resolve(isWindows() ? "protoc.exe" : "protoc");
        Path include = installDir.resolve("include");
        Path marker = installDir.resolve(".dbjo-protoc.marker");

        // fast path
        if (Files.isRegularFile(exe) && Files.isDirectory(include) && markerMatches(marker, protocVersion, platform)) {
            log.info("protoc already installed: " + exe);
            return new ProtocPaths(exe, include, protocVersion, platform);
        }

        if (offline) {
            throw new MojoExecutionException(
                    "Maven is offline and protoc is missing.\n" +
                            "Expected: " + exe + "\n" +
                            "Either run once online or preinstall protoc and set -Dprotoc / -Dprotoc.include."
            );
        }

        try {
            Files.createDirectories(installDir);

            Path tmpZip = installDir.resolve(fileName);
            log.info("Downloading protoc: " + url);
            download(url, tmpZip);

            // clean old (but don't delete installDir itself)
            deleteIfExists(installDir.resolve("bin"));
            deleteIfExists(installDir.resolve("include"));

            log.info("Unzipping: " + tmpZip + " -> " + installDir);
            unzip(tmpZip, installDir);

            if (!Files.isRegularFile(exe)) {
                throw new MojoExecutionException(
                        "protoc zip extracted but executable not found: " + exe + "\n" +
                                "Downloaded from: " + url
                );
            }
            if (!Files.isDirectory(include)) {
                throw new MojoExecutionException(
                        "protoc zip extracted but include dir not found: " + include + "\n" +
                                "Downloaded from: " + url
                );
            }

            ensureExecutable(exe);

            writeMarker(marker, protocVersion, platform);

            log.info("Installed protoc: " + exe);
            return new ProtocPaths(exe, include, protocVersion, platform);

        } catch (IOException | InterruptedException e) {
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

        // parts[0]=major, parts[1]=minor, parts[2]=patch...
        String minor = parts[1];
        String patch = parts[2].replaceAll("\\D.*$", ""); // strip qualifiers if any
        if (minor.isBlank() || patch.isBlank()) return null;

        return minor + "." + patch;
    }

    // internals

    private static void download(URI url, Path out) throws IOException, InterruptedException, MojoExecutionException {
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build();

        HttpRequest req = HttpRequest.newBuilder(url)
                .timeout(Duration.ofMinutes(3))
                .GET()
                .build();

        HttpResponse<InputStream> resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream());
        int code = resp.statusCode();
        if (code < 200 || code >= 300) {
            throw new MojoExecutionException("Download failed: HTTP " + code + " for " + url);
        }

        Files.createDirectories(out.getParent());
        try (InputStream in = resp.body();
             OutputStream os = Files.newOutputStream(out, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            in.transferTo(os);
        }
    }

    private static void unzip(Path zipFile, Path destDir) throws IOException {
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile))) {
            ZipEntry e;
            while ((e = zis.getNextEntry()) != null) {
                Path outPath = destDir.resolve(e.getName()).normalize();
                if (!outPath.startsWith(destDir)) {
                    throw new IOException("Zip traversal attempt: " + e.getName());
                }
                if (e.isDirectory()) {
                    Files.createDirectories(outPath);
                } else {
                    Files.createDirectories(outPath.getParent());
                    try (OutputStream os = Files.newOutputStream(outPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                        zis.transferTo(os);
                    }
                }
                zis.closeEntry();
            }
        }
    }

    private static void ensureExecutable(Path file) throws IOException {
        if (isWindows()) return; // irrelevant
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

    private static boolean markerMatches(Path marker, String v, String platform) {
        if (!Files.isRegularFile(marker)) return false;
        try {
            String s = Files.readString(marker, StandardCharsets.UTF_8).trim();
            return s.equals(v + "|" + platform);
        } catch (IOException e) {
            return false;
        }
    }

    private static void writeMarker(Path marker, String v, String platform) throws IOException {
        Files.writeString(marker, v + "|" + platform, StandardCharsets.UTF_8,
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

    private static String detectPlatform() throws MojoExecutionException {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);

        boolean arm64 = arch.contains("aarch64") || arch.contains("arm64");
        boolean x64 = arch.contains("x86_64") || arch.contains("amd64");

        if (os.contains("win")) {
            if (!x64) throw new MojoExecutionException("Unsupported Windows arch: " + arch + " (only x86_64 supported)");
            return "win64";
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

    private static String trimTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }
}
