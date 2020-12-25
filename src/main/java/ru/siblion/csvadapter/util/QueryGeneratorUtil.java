package ru.siblion.csvadapter.util;

import java.util.List;

public class QueryGeneratorUtil {
    public static String generateParamsForInsert(final int columnsCount) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("?");
        for (int i = 0; i < columnsCount - 1; i++) {
            stringBuilder.append(", ?");
        }
        return stringBuilder.toString();
    }

    public static String generateParamsForUpdate(final int columnsCount, List<String> columnNames) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < columnsCount - 2; i++) {
            stringBuilder.append(columnNames.get(i)).append("=?, ");
        }
        stringBuilder.append(columnNames.get(columnsCount - 1)).append("=? ");
        String id = columnNames.get(0);
        stringBuilder.append("WHERE ").append(id).append("=?");
        return stringBuilder.toString();
    }
}
