package org.github.dbjo.meta.features;

import java.nio.charset.StandardCharsets;

public final class PartitionId {

    private PartitionId() {}

    /**
     * Deterministic partition for ASCII-only keys across:
     *   Java + SQL Server + Sybase ASE + Oracle (+ HSQL via Java routine).
     *
     * Assumptions / requirements:
     *   1) key is ASCII only (U+0000..U+007F). If not, results are NOT guaranteed across DBs
     *      with differing encodings for VARCHAR.
     *   2) Trailing spaces are significant (hashed). This matches the SQL variants below that use
     *      DATALENGTH/LENGTHB (byte length, includes trailing spaces).
     *      If you want to IGNORE trailing spaces, apply RTRIM/TRIM in BOTH Java and every SQL function.
     *   3) parts > 0. Returns NULL for null key or parts<=0.
     *
     * Algorithm:
     *   FNV-1a 32-bit over bytes, then mod parts:
     *     h = 2166136261
     *     for each byte b:
     *       h = (h XOR b) * 16777619  (mod 2^32)
     *     return h % parts           (0..parts-1)
     */
    public static Integer partition(String key, int parts) {
        if (key == null || parts <= 0) {
            return null;
        }

        // Enforce ASCII-only to guarantee cross-DB determinism for VARCHAR.
        for (int i = 0; i < key.length(); i++) {
            if (key.charAt(i) > 0x7F) {
                throw new IllegalArgumentException(
                        "Non-ASCII character at index " + i +
                                " (U+" + String.format("%04X", (int) key.charAt(i)) + ")."
                );
            }
        }

        byte[] bytes = key.getBytes(StandardCharsets.US_ASCII);

        long h = 2166136261L; // unsigned 32-bit
        for (byte bb : bytes) {
            h ^= (bb & 0xffL);
            h = (h * 16777619L) & 0xFFFF_FFFFL; // keep 32-bit
        }

        return (int) (h % (long) parts);
    }

    // =======================
    // SQL Server (MS SQL) UDF
    // =======================
    // Copy/paste this block into SQL Server:
  /*
    CREATE OR ALTER FUNCTION dbo.partition_id(@key varchar(8000), @parts int)
    RETURNS int
    WITH SCHEMABINDING
    AS
    BEGIN
      IF @key IS NULL OR @parts IS NULL OR @parts <= 0
        RETURN NULL;

      DECLARE @h   bigint = 2166136261;
      DECLARE @i   int    = 1;
      DECLARE @len int    = DATALENGTH(@key);  -- bytes, includes trailing spaces

      WHILE @i <= @len
      BEGIN
        DECLARE @b int = ASCII(SUBSTRING(@key, @i, 1));
        SET @h = (@h ^ @b);
        SET @h = (@h * 16777619) % 4294967296; -- mod 2^32
        SET @i += 1;
      END

      RETURN CONVERT(int, @h % @parts);
    END
    GO
  */

    // =============
    // Sybase ASE UDF
    // =============
    // Copy/paste this block into Sybase ASE:
  /*
    create function dbo.partition_id(@key varchar(8000), @parts int)
    returns int
    as
    begin
      if @key is null or @parts is null or @parts <= 0
        return null

      declare @h   numeric(20,0)
      declare @i   int
      declare @len int
      declare @b   int

      select @h   = 2166136261
      select @i   = 1
      select @len = datalength(@key)  -- bytes, includes trailing spaces

      while @i <= @len
      begin
        select @b = ascii(substring(@key, @i, 1))
        select @h = (@h ^ @b)
        select @h = (@h * 16777619) % 4294967296
        select @i = @i + 1
      end

      return convert(int, @h % @parts)
    end
    go
  */

    // =============
    // Oracle FUNCTION
    // =============
    // Copy/paste this block into Oracle:
  /*
    create or replace function partition_id(p_key varchar2, p_parts number)
      return number
      deterministic
    is
      h   number := 2166136261;
      i   pls_integer := 1;
      len pls_integer;
      b   pls_integer;
    begin
      if p_key is null or p_parts is null or p_parts <= 0 then
        return null;
      end if;

      len := lengthb(p_key); -- bytes, includes trailing spaces

      while i <= len loop
        b := ascii(substr(p_key, i, 1));
        h := bitxor(h, b);
        h := mod(h * 16777619, 4294967296); -- mod 2^32
        i := i + 1;
      end loop;

      return mod(h, p_parts);
    end;
    /
  */

    // =====
    // HSQLDB
    // =====
    // Recommended: register this Java method as an HSQLDB SQL function:
  /*
    CREATE FUNCTION PARTITION_ID(key VARCHAR(8000), parts INTEGER)
    RETURNS INTEGER
    LANGUAGE JAVA
    DETERMINISTIC
    EXTERNAL NAME 'CLASSPATH:org.github.dbjo.meta.features.PartitionId.partition';
  */

    // Typical usage (all DBs):
    //   SELECT * FROM t WHERE partition_id(partition_key, :parts) = :p;
}
