package org.github.dbjo.codegen.proto;

import org.github.dbjo.codegen.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

public final class ProtocInvoker {
    private final Config cfg;

    public ProtocInvoker(Config cfg) {
        this.cfg = cfg;
    }

    public void runProtoc(List<Path> protoFiles) throws IOException, InterruptedException {
        Path protoc = cfg.protocPath();
        ensureExecutable(protoc);

        List<String> cmd = new ArrayList<>();
        cmd.add(protoc.toAbsolutePath().toString());

        if (cfg.protoExperimentalOptional()) {
            cmd.add("--experimental_allow_proto3_optional");
        }

        cmd.add("-I" + cfg.protoOutProto().toAbsolutePath());

        if (Files.isDirectory(cfg.protocInclude())) {
            cmd.add("-I" + cfg.protocInclude().toAbsolutePath());
        }

        cmd.add("--java_out=" + cfg.protoOutJava().toAbsolutePath());

        for (Path p : protoFiles) cmd.add(p.toAbsolutePath().toString());

        System.out.println("\nRunning: " + String.join(" ", cmd) + "\n");

        Process proc = new ProcessBuilder(cmd)
                .inheritIO()
                .start();

        int code = proc.waitFor();
        if (code != 0) throw new RuntimeException("protoc failed with exit code " + code);
    }

    private static void ensureExecutable(Path protoc) throws IOException {
        if (!Files.exists(protoc)) {
            throw new IOException("protoc not found: " + protoc.toAbsolutePath());
        }
        if (!Files.isExecutable(protoc)) {
            boolean ok = protoc.toFile().setExecutable(true);
            if (!Files.isExecutable(protoc)) {
                try {
                    Set<PosixFilePermission> perms = EnumSet.of(
                            PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                            PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
                            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE
                    );
                    Files.setPosixFilePermissions(protoc, perms);
                } catch (UnsupportedOperationException ignored) {
                    // non-POSIX FS
                }
            }
            if (!Files.isExecutable(protoc) && !ok) {
                throw new IOException("protoc exists but is not executable: " + protoc.toAbsolutePath());
            }
        }
    }
}
