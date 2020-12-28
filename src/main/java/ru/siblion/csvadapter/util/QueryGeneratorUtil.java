package ru.siblion.csvadapter.util;

import java.util.List;

public class QueryGeneratorUtil {
    //INSERT INTO vk_user_ivan (URL, name) VALUES ('my name', 'my group')
    public static String generateParamsForInsert(final int columnsCount) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("?");
        for (int i = 1; i < columnsCount - 1; i++) {
            stringBuilder.append(", ?");
        }
        return stringBuilder.toString();
    }

    public static String generateParamsForUpdate(final int columnsCount, List<String> columnNames) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 1; i < columnsCount - 2; i++) {
            stringBuilder.append(columnNames.get(i)).append("=?, ");
        }
        stringBuilder.append(columnNames.get(columnsCount - 2)).append("=? ");
        String id = columnNames.get(0);
        stringBuilder.append("WHERE ").append(id).append("=?");
        return stringBuilder.toString();
    }

    public static String generateColumnNamesAndTypesDividedByComa(int columnsCount,
                                                                  List<String> columnNames, List<String> columnTypesNames) {
        StringBuilder stringBuilder = new StringBuilder();
        columnsCount=columnsCount-1;
        for (int i = 0; i < columnsCount-1; i++) {
            stringBuilder.append(columnNames.get(i)).append(" ").append(columnTypesNames.get(i)).append(", ");
        }
        stringBuilder.append(columnNames.get(columnsCount-1)).append(" ").append(columnTypesNames.get(columnsCount-1));
        return stringBuilder.toString();
    }
    public static String generateTableNameDotColumnNamesDividedByComa(final int columnsCount, List<String> columnNames, String tableName){

        StringBuilder stringBuilder = new StringBuilder();
        columnNames.stream().filter(columnName->!columnName.equals("id")).forEach(columnName->stringBuilder.append(tableName).append(".").append(columnName).append(", "));
        stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        return stringBuilder.toString();
    }
    public static String generateColumnNamesDividedByComa(final int columnsCount, List<String> columnNames){
        StringBuilder stringBuilder = new StringBuilder();
        columnNames.stream().filter(columnName->!columnName.equals("id")).forEach(columnName->stringBuilder.append(columnName).append(", "));
        stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        return stringBuilder.toString();
    }
    public static String generateColumnNamesEqualTempTableDotColumnNamesDividedByComa(List<String> columnNames,String tempTableName){
        StringBuilder stringBuilder = new StringBuilder();
        columnNames.stream()
            .filter(columnName->!columnName.equals("id"))
            .forEach(columnName->stringBuilder
                .append(columnName)
                .append(" = ")
                .append(tempTableName).append(".").append(columnName)
                .append(", "));
        stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        return stringBuilder.toString();
    }
}
