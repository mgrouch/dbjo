package org.github.dbjo.codegen.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

public final class FilesUtil {
    private FilesUtil() {}

    public static void writeString(Path file, String content, boolean overwrite) throws IOException {
        Files.createDirectories(file.getParent());
        if (!overwrite && Files.exists(file)) {
            System.out.println("Skip (exists): " + file);
            return;
        }
        Files.writeString(
                file,
                content,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        );
    }
}
