package org.github.dbjo.rdb.demo;

import org.rocksdb.*;

public class RocksSmoke {
    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();

        try (Options opt = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opt, "D:/tmp/rocks-smoke")) {

            db.put("k".getBytes(), "v".getBytes());
            byte[] v = db.get("k".getBytes());
            System.out.println(new String(v));
        }
    }
}
