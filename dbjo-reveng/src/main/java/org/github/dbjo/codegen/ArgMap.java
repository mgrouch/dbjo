package org.github.dbjo.codegen;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

public final class ArgMap {
    private final Map<String, String> map;

    private ArgMap(Map<String, String> map) {
        this.map = map;
    }

    public static ArgMap parse(String[] args) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--")) continue;

            String key;
            String val;

            int eq = a.indexOf('=');
            if (eq >= 0) {
                key = a.substring(2, eq).trim();
                val = a.substring(eq + 1).trim();
            } else {
                key = a.substring(2).trim();
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    val = args[++i];
                } else {
                    val = "true";
                }
            }
            if (!key.isEmpty()) m.put(key, val);
        }
        return new ArgMap(m);
    }

    public String get(String key, String def) {
        String v = map.get(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    public boolean getBool(String key, boolean def) {
        String v = map.get(key);
        if (v == null) return def;
        return switch (v.trim().toLowerCase(Locale.ROOT)) {
            case "1", "true", "yes", "y", "on" -> true;
            case "0", "false", "no", "n", "off" -> false;
            default -> def;
        };
    }

    public Pattern getRegex(String key, Pattern def) {
        String v = map.get(key);
        if (v == null || v.isBlank()) return def;
        return Pattern.compile(v);
    }
}
