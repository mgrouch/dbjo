package org.github.dbjo.codegen.registry;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.model.TableModel;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Generates ONE class:
 *
 * package <metaPkg>.registry;
 *
 * import <beanPkg>.<Entity>;
 * import <metaPkg>.<Entity>Meta;
 * import org.github.dbjo.meta.entity.DefaultMetaRegistry;
 *
 * public final class GeneratedMetaRegistry {
 *   private GeneratedMetaRegistry() {}
 *
 *   public static DefaultMetaRegistry create() {
 *     DefaultMetaRegistry r = new DefaultMetaRegistry();
 *     r.register(Entity.class.getName(), EntityMeta._META);
 *     return r;
 *   }
 * }
 */
public final class MetaRegistryGenerator {
    private static final String DEFAULT_REGISTRY_CLASS = "GeneratedMetaRegistry";
    private static final String DEFAULT_DEFAULT_META_REGISTRY_FQN = "org.github.dbjo.meta.entity.DefaultMetaRegistry";
    private static final String DEFAULT_META_SUFFIX = "Meta";

    private final Config cfg;

    public MetaRegistryGenerator(Config cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    /** @return 1 if written, 0 if no tables */
    public int generate(List<TableModel> tables) throws IOException {
        if (tables == null || tables.isEmpty()) return 0;

        String metaPkg = cfg.metaPkg();
        String beanPkg = cfg.beanPkg();

        // Optional overrides via system properties
        //  -Ddbjo.registryPkg=...
        //  -Ddbjo.registryClass=...
        //  -Ddbjo.defaultMetaRegistryFqn=...
        //  -Ddbjo.metaSuffix=Meta
        String registryPkg = sys("dbjo.registryPkg", metaPkg + ".registry");
        String registryClass = sys("dbjo.registryClass", DEFAULT_REGISTRY_CLASS);
        String defaultMetaRegistryFqn = sys("dbjo.defaultMetaRegistryFqn", DEFAULT_DEFAULT_META_REGISTRY_FQN);
        String metaSuffix = sys("dbjo.metaSuffix", DEFAULT_META_SUFFIX);

        Path outDir = cfg.codegenOutJava().resolve(registryPkg.replace('.', '/'));
        Files.createDirectories(outDir);

        // Build deterministic list
        List<Entry> entries = new ArrayList<>(tables.size());
        for (TableModel tm : tables) {
            String entityClass = Naming.toClassName(tm.table().table());
            String metaClass = entityClass + metaSuffix;
            entries.add(new Entry(entityClass, metaClass));
        }
        entries.sort(Comparator.comparing(Entry::entityClass));

        String src = render(registryPkg, registryClass, defaultMetaRegistryFqn, beanPkg, metaPkg, entries);

        Path outFile = outDir.resolve(registryClass + ".java");
        FilesUtil.writeString(outFile, src, cfg.overwrite());
        System.out.println("Wrote: " + outFile);
        return 1;
    }

    private static String render(
            String registryPkg,
            String registryClass,
            String defaultMetaRegistryFqn,
            String beanPkg,
            String metaPkg,
            List<Entry> entries
    ) {
        String defaultMetaRegistrySimple = simpleName(defaultMetaRegistryFqn);

        // Imports (stable order)
        Set<String> imports = new TreeSet<>();
        imports.add(defaultMetaRegistryFqn);

        for (Entry e : entries) {
            imports.add(beanPkg + "." + e.entityClass());
            imports.add(metaPkg + "." + e.metaClass());
        }

        StringBuilder sb = new StringBuilder(4096);
        sb.append("package ").append(registryPkg).append(";\n\n");
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        sb.append("\n");

        sb.append("public final class ").append(registryClass).append(" {\n");
        sb.append("  private ").append(registryClass).append("() {}\n\n");

        sb.append("  public static ").append(defaultMetaRegistrySimple).append(" create() {\n");
        sb.append("    ").append(defaultMetaRegistrySimple).append(" r = new ").append(defaultMetaRegistrySimple).append("();\n");

        for (Entry e : entries) {
            sb.append("    r.register(")
                    .append(e.entityClass()).append(".class.getName(), ")
                    .append(e.metaClass()).append("._META")
                    .append(");\n");
        }

        sb.append("    return r;\n");
        sb.append("  }\n");
        sb.append("}\n");
        return sb.toString();
    }

    private static String sys(String key, String def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        return v.isEmpty() ? def : v;
    }

    private static String simpleName(String fqn) {
        int i = fqn.lastIndexOf('.');
        return (i >= 0) ? fqn.substring(i + 1) : fqn;
    }

    private record Entry(String entityClass, String metaClass) {}
}
