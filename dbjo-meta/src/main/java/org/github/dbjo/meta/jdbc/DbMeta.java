// File: src/main/java/org/github/dbjo/meta/jdbc/DbMeta.java
package org.github.dbjo.meta.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;

public interface DbMeta<T> {
    String schema();
    String table();
    String fqn();

    String insertSql();
    String updateByIdSql();
    String selectAllSql();

    Object[] insertParams(T e);
    SQLType[] insertParamTypes();

    Object[] updateByIdParams(T e);
    SQLType[] updateByIdParamTypes();

    T fromRow(ResultSet rs) throws SQLException;
}
