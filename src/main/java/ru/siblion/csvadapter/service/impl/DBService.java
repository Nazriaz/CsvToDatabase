package ru.siblion.csvadapter.service.impl;

import ru.siblion.csvadapter.config.DataBaseConfig;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

public class DBService {
    private String table;
    private DataBaseConfig dataBaseConfig;

    public DBService(String table, DataBaseConfig dataBaseConfig) {
        this.table = table;
        this.dataBaseConfig = dataBaseConfig;
    }

    public void insertIntoTable(List<String[]> records, final int columnsCount) throws SQLException {

        Connection connection = dataBaseConfig.dataSource().getConnection();
        final String insertQuery = format("INSERT INTO %s VALUES(%s)", table, generateParamsForInsert(columnsCount));
        List<String> columnNames = getColumnNames(connection);
        final String updateQuery = format("UPDATE %s SET %s", table, generateParamsForUpdate(columnsCount, columnNames));

//        PreparedStatement preparedStatement = connection.prepareStatement(updateQuery);
        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

        System.out.println(updateQuery);
        for (final String[] record : records) {
            for (int i = 0; i < columnsCount; i++) {
                preparedStatement.setString(i + 1, record[i]);
            }
            try {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
            }
        }
//        int[] ints = preparedStatement.executeBatch();
//        for (int anInt : ints) {
//            System.out.println(anInt);
//        }
        preparedStatement.close();
        connection.close();
    }

    private String generateParamsForInsert(final int columnsCount) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("?");
        for (int i = 0; i < columnsCount - 1; i++) {
            stringBuilder.append(", ?");
        }
        return stringBuilder.toString();
    }
//    UPDATE Messages SET description = ?, author = ? WHERE id = ? AND seq_num = ?"
    private String generateParamsForUpdate(final int columnsCount,List<String> columnNames) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < columnsCount - 2; i++) {
            stringBuilder.append(columnNames.get(i)).append("='?', ");
        }
        stringBuilder.append(columnNames.get(columnsCount-1)).append("='?' ");
        String id = columnNames.get(0);
        stringBuilder.append("WHERE ").append(id).append("='?'");
        return stringBuilder.toString();
    }

    private List<String> getColumnNames(Connection connection) {
        final String query = format("SELECT * FROM %s FETCH FIRST 1 ROWS ONLY", table);
        LinkedList<String> columnNames = new LinkedList<>();
        try (ResultSet rs = connection.createStatement().executeQuery(query)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                columnNames.add(rsmd.getColumnName(i + 1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnNames;
    }
}
