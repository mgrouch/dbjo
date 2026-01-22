package org.github.dbjo.meta.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;

public interface DbMeta<T> {
    /** INSERT statement for non-auto-increment columns. */
    String insertSql();

    /** UPDATE statement by PK. */
    String updateByIdSql();

    /** SELECT all columns in stable order (same order as fromRow reads). */
    String selectAllSql();

    /** Map a single row (columns must match selectAllSql order). */
    T fromRow(ResultSet rs) throws SQLException;

    /** Parameters for INSERT in the same order as insertSql placeholders. */
    Object[] insertParams(T e);

    /** SQL types for INSERT params (same length/order as insertParams). */
    SQLType[] insertParamTypes();

    /** Parameters for UPDATE ... WHERE pk=? in placeholder order. */
    Object[] updateByIdParams(T e);

    /** SQL types for UPDATE params (same length/order as updateByIdParams). */
    SQLType[] updateByIdParamTypes();
}
