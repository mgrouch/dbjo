package org.github.dbjo.criteria.cache;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.github.dbjo.criteria.spec.QuerySpec;

public final class QueryCacheKeyFactory {
    private QueryCacheKeyFactory() {}

    public static QueryCacheKey from(QuerySpec spec) {
        QuerySpec canon = SpecCanonicalizer.canonicalize(spec);
        String s = SpecStringifier.stableKey(canon);
        return new QueryCacheKey(s, sha256Hex(s));
    }

    private static String sha256Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(dig.length * 2);
            for (byte b : dig) sb.append(Character.forDigit((b >>> 4) & 0xF, 16))
                    .append(Character.forDigit(b & 0xF, 16));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
