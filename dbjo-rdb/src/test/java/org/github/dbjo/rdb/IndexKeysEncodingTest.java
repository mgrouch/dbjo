package org.github.dbjo.rdb;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class IndexKeysEncodingTest {

    @Test
    void uniqueRoundTripsPkEvenWhenValueContainsZeros() {
        byte[] value = new byte[] { 0x12, 0x00, (byte) 0xAB, 0x00, 0x7F };
        byte[] pk = new byte[] { 0x01, 0x02, 0x03 };

        byte[] idxKey = IndexKeys.unique(value, pk);

        int sepPos = IndexKeys.sepPos(idxKey);
        assertTrue(sepPos >= 0, "separator not found");

        byte[] pk2 = IndexKeys.pkFromIndexKey(idxKey);
        assertArrayEquals(pk, pk2);

        // Escaped value part must not contain SEP (0x00 0x00)
        byte[] ev = IndexKeys.escapedValuePart(idxKey);
        assertNotNull(ev);

        for (int i = 0; i < ev.length - 1; i++) {
            assertFalse(ev[i] == 0x00 && ev[i + 1] == 0x00, "escaped value contains separator");
        }
    }

    @Test
    void escapingPreservesUnsignedLexicographicOrdering() {
        Random rnd = new Random(12345);

        for (int i = 0; i < 20_000; i++) {
            byte[] a = randomBytes(rnd, rnd.nextInt(16));
            byte[] b = randomBytes(rnd, rnd.nextInt(16));

            int expected = sign(ByteArrays.compare(a, b));
            int got = sign(ByteArrays.compare(IndexKeys.escapeValue(a), IndexKeys.escapeValue(b)));

            assertEquals(expected, got, "ordering mismatch");
        }
    }

    private static int sign(int x) {
        return Integer.compare(x, 0);
    }

    private static byte[] randomBytes(Random rnd, int n) {
        byte[] b = new byte[n];
        rnd.nextBytes(b);
        // ensure we see lots of zeros
        if (n > 0 && rnd.nextInt(5) == 0) {
            b[rnd.nextInt(n)] = 0x00;
        }
        return b;
    }
}
