package ru.siblion.csvadapter.service.impl;

import ru.siblion.csvadapter.config.ConnectionPool;
import ru.siblion.csvadapter.util.DbMetaDataUtil;
import ru.siblion.csvadapter.util.ProcessingTimer;
import ru.siblion.csvadapter.util.QueryGeneratorUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.lang.String.format;

public class NewDBService {
    private String tableName;

    public NewDBService(String tableName) {
        this.tableName = tableName;
    }

    public void createTempTable() {
        String query = prepareCreateTempTableQuery();
        System.out.println(query);
        try (Connection connection = ConnectionPool.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<String[]> insertToNewTableQuery(List<String[]> records) {

        ProcessingTimer processingTimer = new ProcessingTimer();
        processingTimer.start();
        List<String[]> strings = new ArrayList<>();
        //todo делать что то с этим например можно к названию таблицы добавить _temp
        String newTableName = "newvals";
        int columnsCount = DbMetaDataUtil.getColumnCount(newTableName);
        List<String> columnNames = DbMetaDataUtil.getColumnNames(newTableName);
        int[] columnMaxSize = DbMetaDataUtil.getColumnMaxSize(tableName);
        List<String[]> collect = verifyRecords(records, columnMaxSize);
        //todo
        preparedStatementExecution(collect, columnsCount, newTableName, columnNames);
        System.out.println(processingTimer.stop() + " time to insert");
        return strings;

    }

    private void mergeTables() {
    }
    //CREATE TEMPORARY TABLE newvals(id integer, somedata text);

    private String prepareCreateTempTableQuery() {
        String tempTableCreationQuery = "";
        List<String> columnNamesWithoutId = DbMetaDataUtil.getColumnNamesWithoutId(tableName);
//        List<String> columnTypeNamesWithoutId = DbMetaDataUtil.getColumnTypeNamesWithoutId(tableName);
        int columnsCount = DbMetaDataUtil.getColumnCount(tableName);
        String createTableParams = QueryGeneratorUtil.generateColumnNamesDividedByComa(columnsCount, columnNamesWithoutId);
        //todo закомитен альтернативный вариант где создаётся таблица без учёта длинны полей в оргигинальной таблице
//            String createTableParams = QueryGeneratorUtil
//                .generateColumnNamesAndTypesDividedByComa(columnsCount, columnNamesWithoutId, columnTypeNamesWithoutId);
        //            tempTableCreationQuery = format("DROP TABLE IF EXISTS newvals; CREATE TABLE newvals (%s)", createTableParams);
        tempTableCreationQuery = format("DROP TABLE IF EXISTS newvals; CREATE TABLE newvals AS SELECT %s FROM %s;DELETE FROM newvals;", createTableParams, tableName);

        return tempTableCreationQuery;
    }

    private void prepareInsertToNewTableQuery() {
    }

    public void prepareMergeTablesQuery() {
        ProcessingTimer processingTimer = new ProcessingTimer();
        processingTimer.start();
        try (Connection connection = ConnectionPool.getConnection()) {
            String tempTableName = "newvals";
            int columnsCount = DbMetaDataUtil.getColumnCount(tempTableName);
            List<String> columnNames = DbMetaDataUtil.getColumnNames(tempTableName);
            String tempTableNameDotColumnNamesDividedByComa = QueryGeneratorUtil.generateTableNameDotColumnNamesDividedByComa(columnsCount, columnNames, tempTableName);
            String columnNamesDividedByComa = QueryGeneratorUtil.generateColumnNamesDividedByComa(columnsCount, columnNames);
            String id = "url";

            String H2UpdateQuery = "BEGIN;UPDATE " + tableName + " target SET (" + QueryGeneratorUtil.generateColumnNamesDividedByComa(columnsCount, columnNames) +
                ") =  (SELECT " + QueryGeneratorUtil.generateColumnNamesDividedByComa(columnsCount, columnNames) + " FROM " + tempTableName + " source where source." + id + " = target." + id + ");";
            String postgreUpdateQuery = "Begin;UPDATE " + tableName + " SET " + QueryGeneratorUtil.generateColumnNamesEqualTempTableDotColumnNamesDividedByComa(columnNames, tempTableName) + " FROM " + tempTableName + " WHERE " + tempTableName + "." + id + " = " + tableName + "." + id + ";";
            String insertQuery =
                "INSERT INTO " + tableName + " (" + columnNamesDividedByComa + ") " +
                    "SELECT " + tempTableNameDotColumnNamesDividedByComa + " " +
                    "FROM " + tempTableName + " LEFT OUTER JOIN " + tableName + " " +
                    "ON (" + tableName + "." + id + " = " + tempTableName + "." + id + ") " +
                    "WHERE " + tableName + "." + id + " IS NULL;";
            String dropQuery = "DROP TABLE " + tempTableName + "; " +
                "COMMIT;";
//            String query = "" +
//                "UPDATE vk_user_ivan\n" +
//                "SET url = newvals.url\n" +
//                "FROM newvals\n" +
//                "WHERE newvals.url = vk_user_ivan.url;\n" +
//                "\n" +
//                "INSERT INTO vk_user_ivan (url,name)\n" +
//                "SELECT newvals.url, newvals.name\n" +
//                "FROM newvals\n" +
//                "LEFT OUTER JOIN vk_user_ivan ON (vk_user_ivan.url = newvals.url)\n" +
//                "WHERE vk_user_ivan.url IS NULL;\n" +
//                "DROP TABLE newvals;\n" +
//                "\n" +
//                "COMMIT;";
            System.out.println(insertQuery);
            String finalQuery = new StringBuilder()
                .append(postgreUpdateQuery)
                .append(insertQuery)
                .append(dropQuery)
                .toString();
            System.out.println();
            System.out.println(finalQuery);
            try (final PreparedStatement preparedStatement = connection.prepareStatement(finalQuery);) {
                preparedStatement.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(processingTimer.stop() + " MERGING TIME");
    }

    private void preparedStatementExecution(List<String[]> records,
                                            int columnsCount, String newTableName, List<String> columnNames) {
        //todo пофиксить стринг как то ещё помимо субстринга
        final String insertQuery = format("INSERT INTO %s (%s) VALUES(%s)", newTableName, columnNames.toString()
            .substring(1, columnNames.toString().length() - 1), QueryGeneratorUtil.generateParamsForInsert(columnsCount + 1));

        try (final Connection connection = ConnectionPool.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
            int catcher = 0;
            int batcher = 0;
            for (final String[] record : records) {
                batcher++;
                for (int i = 0; i < columnsCount; i++) {
                    preparedStatement.setString(i + 1, record[i]);
                }
                preparedStatement.addBatch();
                if (batcher % 10000 == 0) {
                    preparedStatement.executeBatch();
                    System.out.println("batch worked" + batcher / 100);
                }
            }
            preparedStatement.executeBatch();
            System.out.println(catcher + " issues caught");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<String[]> verifyRecords(List<String[]> records, int[] columnMaxSize) {
        ProcessingTimer processingTimer = new ProcessingTimer();
        processingTimer.start();
        System.out.println(records.size());
        HashMap<String, String[]> stringHashMap = new HashMap<>();
        //todo id уникальные значения по которым понимать уникальность строки таблицы можно передать тут
        records.stream()
            .filter(strings -> strings.length == 24)
            .filter(record -> checkColumnSize(record, columnMaxSize))
            //todo убрать https ни на что не влияет
            .filter(record -> record[0].startsWith("https"))
            .forEach(a -> stringHashMap.put(a[0], a));
        List<String[]> strings = new ArrayList<>(stringHashMap.values());
        System.out.println(strings.size());
        double stop = processingTimer.stop();
        System.out.println(stop);
        return strings;
    }

    public boolean checkColumnSize(String[] record, int[] columnsSize) {
        if (record.length != columnsSize.length) return false;
        for (int i = 0; i < record.length; i++) {
            if (record[i].length() > columnsSize[i]) return false;
        }
        return true;
    }
}
