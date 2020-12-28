package ru.siblion.csvadapter.util;

import ru.siblion.csvadapter.config.ConnectionPool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

public class DbMetaDataUtil {
    public static List<String> getColumnNames(String tableName) {
        final String query = format("SELECT * FROM %s FETCH FIRST 1 ROWS ONLY", tableName);
        LinkedList<String> columnNames = new LinkedList<>();
        try (final Connection connection = ConnectionPool.getConnection();
             ResultSet rs = connection.createStatement().executeQuery(query)) {
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
    public static List<String> getColumnNamesWithoutId(String tableName) {
        final String query = format("SELECT * FROM %s FETCH FIRST 1 ROWS ONLY", tableName);
        LinkedList<String> columnNames = new LinkedList<>();
        try (final Connection connection = ConnectionPool.getConnection();
             ResultSet rs = connection.createStatement().executeQuery(query)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i < columnCount; i++) {
                columnNames.add(metaData.getColumnName(i + 1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnNames;
    }

    public static int getColumnCount(String tableName) {
        int columnCount = 0;
        final String query = format("SELECT * FROM %s FETCH FIRST 1 ROWS ONLY", tableName);
        try (final Connection connection = ConnectionPool.getConnection();
             ResultSet rs = connection.createStatement().executeQuery(query)) {
            ResultSetMetaData metaData = rs.getMetaData();
            columnCount = metaData.getColumnCount();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnCount;
    }
    public static List<String> getColumnTypeNamesWithoutId(String tableName){
        final String query = format("SELECT * FROM %s FETCH FIRST 1 ROWS ONLY", tableName);
        LinkedList<String> columnTypeNames = new LinkedList<>();
        try (final Connection connection = ConnectionPool.getConnection();
             ResultSet rs = connection.createStatement().executeQuery(query)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i < columnCount; i++) {
                columnTypeNames.add(metaData.getColumnTypeName(i + 1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnTypeNames;
    }
    public static int[] getColumnMaxSize(String tableName){
        final String query = format("SELECT * FROM %s FETCH FIRST 1 ROWS ONLY", tableName);
        int[] columnMaxSize=new int[0];
        try (final Connection connection = ConnectionPool.getConnection();
             ResultSet rs = connection.createStatement().executeQuery(query)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            columnMaxSize = new int[columnCount-1];
            for (int i = 1; i < columnCount; i++) {
                int columnDisplaySize = metaData.getColumnDisplaySize(i + 1);
                columnMaxSize[i-1]= columnDisplaySize;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnMaxSize;
    }
}
