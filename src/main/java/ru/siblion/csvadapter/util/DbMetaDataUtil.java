package ru.siblion.csvadapter.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

public class DbMetaDataUtil {
    public static List<String> getColumnNames(Connection connection, String tableName) {
        final String query = format("SELECT * FROM %s FETCH FIRST 1 ROWS ONLY", tableName);
        LinkedList<String> columnNames = new LinkedList<>();
        try (ResultSet rs = connection.createStatement().executeQuery(query)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                columnNames.add(metaData.getColumnName(i + 1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnNames;
    }

    public static int getColumnCount(Connection connection, String tableName) {
        int columnCount = 0;
        final String query = format("SELECT * FROM %s FETCH FIRST 1 ROWS ONLY", tableName);
        try (ResultSet rs = connection.createStatement().executeQuery(query)) {
            ResultSetMetaData metaData = rs.getMetaData();
            columnCount = metaData.getColumnCount();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnCount;
    }
}
