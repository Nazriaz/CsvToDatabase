package ru.siblion.csvadapter.config;

import org.h2.jdbcx.JdbcDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;

public class DataBaseConfig {
    private String dbConnectionString;
    private String username;
    private String password;

    public DataBaseConfig(String dbConnectionString, String username, String password) {
        this.dbConnectionString = dbConnectionString;
        this.username = username;
//        this.password = "";
        this.password = password;
    }

    public DataSource getDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        JdbcDataSource dataSource2 = new JdbcDataSource();
        dataSource.setURL(dbConnectionString);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        return dataSource;
    }
}
