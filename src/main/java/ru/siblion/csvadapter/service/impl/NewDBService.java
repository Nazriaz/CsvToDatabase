package ru.siblion.csvadapter.service.impl;

import ru.siblion.csvadapter.config.DataBaseConfig;
import ru.siblion.csvadapter.util.DbMetaDataUtil;
import ru.siblion.csvadapter.util.ProcessingTimer;
import ru.siblion.csvadapter.util.QueryGeneratorUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.lang.String.format;

public class NewDBService {
    private String tableName;
    private DataBaseConfig dataBaseConfig;

    public NewDBService(String tableName, DataBaseConfig dataBaseConfig) {
        this.tableName = tableName;
        this.dataBaseConfig = dataBaseConfig;
    }

    public void createTempTable() {
        try (Connection connection = dataBaseConfig.getDataSource().getConnection()) {
            String query = prepareCreateTempTableQuery();
            System.out.println(query);
            PreparedStatement preparedStatement = connection.prepareStatement(query);
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
        try (Connection connection = dataBaseConfig.getDataSource().getConnection()) {
            int columnsCount = DbMetaDataUtil.getColumnCount(connection, newTableName);
//            int columnsCount = DbMetaDataUtil.getColumnCount(connection, tableName);
            List<String> columnNames = DbMetaDataUtil.getColumnNames(connection, newTableName);
            int[] columnMaxSize = DbMetaDataUtil.getColumnMaxSize(connection, tableName);
            List<String[]> collect = verifyRecords(records,columnMaxSize);
//            List<String> columnNames = DbMetaDataUtil.getColumnNamesWithoutId(connection, newTableName);
            //todo
            final String insertQuery = format("INSERT INTO %s (%s) VALUES(%s)", newTableName, columnNames.toString().substring(1, columnNames.toString().length() - 1), QueryGeneratorUtil.generateParamsForInsert(columnsCount + 1));
            System.out.println(insertQuery);
            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

            preparedStatementExecution(collect, preparedStatement, columnsCount);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(processingTimer.stop() +" time to insert");
        return strings;

    }

    private void mergeTables() {
    }
    //CREATE TEMPORARY TABLE newvals(id integer, somedata text);

    private String prepareCreateTempTableQuery() {
        String tempTableCreationQuery = "";
        try (Connection connection = dataBaseConfig.getDataSource().getConnection()) {
            List<String> columnNamesWithoutId = DbMetaDataUtil.getColumnNamesWithoutId(connection, tableName);
            List<String> columnTypeNamesWithoutId = DbMetaDataUtil.getColumnTypeNamesWithoutId(connection, tableName);
            int columnsCount = DbMetaDataUtil.getColumnCount(connection, tableName);
            String createTableParams = QueryGeneratorUtil.generateColumnNamesDividedByComa(columnsCount, columnNamesWithoutId);
//            String createTableParams = QueryGeneratorUtil
//                .generateColumnNamesAndTypesDividedByComa(columnsCount, columnNamesWithoutId, columnTypeNamesWithoutId);
            tempTableCreationQuery = format("DROP TABLE IF EXISTS newvals; CREATE TABLE newvals AS SELECT %s FROM %s", createTableParams,tableName);
//            tempTableCreationQuery = format("DROP TABLE IF EXISTS newvals; CREATE TABLE newvals (%s)", createTableParams);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tempTableCreationQuery;
    }

    //INSERT INTO newvals(id, somedata) VALUES (2, 'Joe'), (3, 'Alan');
    private void prepareInsertToNewTableQuery() {
    }

    public void prepareMergeTablesQuery() {
        ProcessingTimer processingTimer = new ProcessingTimer();
        processingTimer.start();
        try (Connection connection = dataBaseConfig.getDataSource().getConnection()) {
            String tempTableName = "newvals";
            int columnsCount = DbMetaDataUtil.getColumnCount(connection, tempTableName);
            List<String> columnNames = DbMetaDataUtil.getColumnNames(connection, tempTableName);
            String tempTableNameDotColumnNamesDividedByComa = QueryGeneratorUtil.generateTableNameDotColumnNamesDividedByComa(columnsCount, columnNames, tempTableName);
            String columnNamesDividedByComa = QueryGeneratorUtil.generateColumnNamesDividedByComa(columnsCount, columnNames);
            String id = "url";
            String query = "Begin;" +
                "UPDATE "+tableName+" SET "+id+" = "+tempTableName+"."+id+" FROM "+tempTableName+" WHERE "+tempTableName+"."+id+" = "+tableName+"."+id+"; " +
                "INSERT INTO "+tableName+" ("+columnNamesDividedByComa+") " +
                "SELECT %s " +
                "FROM "+tempTableName+" LEFT OUTER JOIN "+tableName+" " +
                "ON ("+tableName+"."+id+" = "+tempTableName+"."+id+") " +
                "WHERE "+tableName+"."+id+" IS NULL; " +
                "DROP TABLE "+tempTableName+"; " +
                "COMMIT;";
            String updateQuery = "UPDATE "+tableName+" SET "+QueryGeneratorUtil.generateColumnNamesEqualTempTableDotColumnNamesDividedByComa(columnNames,tempTableName)+" FROM "+tempTableName+" WHERE "+tempTableName+"."+id+" = "+tableName+"."+id+";";
//            String updateQuery = "UPDATE "+tableName+" SET "+id+" = "+tempTableName+"."+id+" FROM "+tempTableName+" WHERE "+tempTableName+"."+id+" = "+tableName+"."+id+";";
//            System.out.println(updateQuery);
            String insertQuery =
                "INSERT INTO "+tableName+" ("+columnNamesDividedByComa+") " +
                "SELECT "+tempTableNameDotColumnNamesDividedByComa+" " +
                "FROM "+tempTableName+" LEFT OUTER JOIN "+tableName+" " +
                "ON ("+tableName+"."+id+" = "+tempTableName+"."+id+") " +
                "WHERE "+tableName+"."+id+" IS NULL;";
            String dropQuery = "DROP TABLE "+tempTableName+"; " +
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
            String finalQuery= new StringBuilder()
                .append(updateQuery)
                .append(insertQuery)
                .append(dropQuery)
                .toString();
            System.out.println();
            System.out.println(insertQuery);
            connection.createStatement().execute(finalQuery);
            System.out.println("JOPA JOPA");
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getNextException());
            System.out.println(e.getSQLState());
        }
        System.out.println(processingTimer.stop()+" MERGING TIME");
    }

    private void preparedStatementExecution(List<String[]> records, PreparedStatement preparedStatement,
                                            int columnsCount) throws SQLException {
        int catcher = 0;
        int batcher = 0;
        for (final String[] record : records) {
            batcher++;
            for (int i = 0; i < columnsCount; i++) {
                preparedStatement.setString(i + 1, record[i]);
            }
            try {
                preparedStatement.addBatch();
//                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                catcher++;
            }
            if (batcher%10000==0) {
                preparedStatement.executeBatch();
                System.out.println("Jopa" + batcher/100);
            }
        }
        preparedStatement.executeBatch();
        System.out.println(catcher + " issues caught");
    }

    public List<String[]> verifyRecords(List<String[]> records, int[] columnMaxSize) {
        ProcessingTimer processingTimer = new ProcessingTimer();
        processingTimer.start();
        System.out.println(records.size());
        HashMap<String, String[]> stringHashMap = new HashMap<>();
        //todo id уникальные значения по которым понимать уникальность строки таблицы можно передать тут
        records.stream()
            .filter(strings -> strings.length==24)
            .filter(record-> checkColumnSize(record,columnMaxSize))
            .forEach(a -> stringHashMap.put(a[0], a));
        List<String[]> strings = new ArrayList<>(stringHashMap.values());
        System.out.println(strings.size());
        double stop = processingTimer.stop();
        System.out.println(stop);
        Arrays.stream(strings.get(0)).forEach(System.out::print);
        return strings;
    }
    public boolean checkColumnSize(String[] record, int[] columnsSize){
//        System.out.println(record.length +" = "+columnsSize.length);
        if (record.length!=columnsSize.length) return false;
        for (int i = 0; i < record.length; i++) {
            if (record[i].length()>columnsSize[i]) return false;
        }
        return true;
    }
}
