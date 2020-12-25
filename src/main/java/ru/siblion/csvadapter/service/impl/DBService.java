package ru.siblion.csvadapter.service.impl;

import ru.siblion.csvadapter.config.DataBaseConfig;
import ru.siblion.csvadapter.util.DbMetaDataUtil;
import ru.siblion.csvadapter.util.QueryGeneratorUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class DBService {
    private String tableName;
    private DataBaseConfig dataBaseConfig;

    public DBService(String tableName, DataBaseConfig dataBaseConfig) {
        this.tableName = tableName;
        this.dataBaseConfig = dataBaseConfig;
    }

    public List<String[]> insertIntoTable(List<String[]> records) throws SQLException {

        Connection connection = dataBaseConfig.getDataSource().getConnection();
        int columnsCount = DbMetaDataUtil.getColumnCount(connection, tableName);
        final String insertQuery = format("INSERT INTO %s VALUES(%s)", tableName, QueryGeneratorUtil.generateParamsForInsert(columnsCount));
        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

        List<String[]> strings = preparedStatementExecution(records, preparedStatement, columnsCount);
        preparedStatement.close();
        connection.close();
        return strings;
    }

    public void updateTable(List<String[]> records) {
        System.out.println("There is " + records.size() + " to update");
        try (Connection connection = dataBaseConfig.getDataSource().getConnection()) {
            int columnsCount = DbMetaDataUtil.getColumnCount(connection, tableName);
            List<String> columnNames = DbMetaDataUtil.getColumnNames(connection, tableName);
            final String updateQuery = format("UPDATE %s SET %s", tableName, QueryGeneratorUtil.generateParamsForUpdate(columnsCount, columnNames));
            System.out.println(updateQuery);
            executeUpdate(records, connection, updateQuery, columnsCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void executeUpdate(List<String[]> records, Connection connection, String updateQuery, int columnsCount) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(updateQuery);) {
            for (final String[] record : records) {
                for (int i = 0; i < columnsCount; i++) {
                    preparedStatement.setString(i + 1, record[i]);
                }
                preparedStatement.setString(columnsCount, record[0]);
                try {
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private List<String[]> preparedStatementExecution(List<String[]> records, PreparedStatement preparedStatement,
                                                      int columnsCount) throws SQLException {
        int catcher = 0;
        List<String[]> stringsToUpdate = new ArrayList<>();
        for (final String[] record : records) {
            for (int i = 0; i < columnsCount; i++) {
                preparedStatement.setString(i + 1, record[i]);
            }
            try {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                stringsToUpdate.add(record);
                catcher++;
            }
        }
        System.out.println(catcher + " issue catched");
        return stringsToUpdate;
    }
}
