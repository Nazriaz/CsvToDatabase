package ru.siblion.csvadapter.util;



import org.junit.jupiter.api.Test;

import java.util.ArrayList;

class QueryGeneratorUtilTest {

    @Test
    void generateParamsForSelect() {
        String s = QueryGeneratorUtil.generateTableNameDotColumnNamesDividedByComa(1, new ArrayList<>() {{
            this.add("Jopa");
            this.add("Jopa");
            this.add("id");
            this.add("Jopa");
        }}, "TableName");
        System.out.println(s);
    }
}
