package org.github.dbjo.meta.jdbc;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcUtil {
    public static void printResultSet(PrintStream out, ResultSet resultSet) throws SQLException {
        ResultSetMetaData meta = resultSet.getMetaData();
        int columns = meta.getColumnCount();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= columns; i++) {
            if (i > 1) {
                header.append(" | ");
            }
            header.append(meta.getColumnLabel(i));
        }
        out.println(header);
        while (resultSet.next()) {
            StringBuilder row = new StringBuilder();
            for (int i = 1; i <= columns; i++) {
                if (i > 1) {
                    row.append(" | ");
                }
                row.append(resultSet.getString(i));
            }
            out.println(row);
        }
    }
}
