package ru.siblion.csvadapter.config;

import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;

public class DataBaseConfig {
    private String dbConnectionString;
    private String username;
    private String password;

    public DataBaseConfig(String dbConnectionString, String username, String password) {
        this.dbConnectionString = dbConnectionString;
        this.username = username;
        this.password = password;
    }

    public DataSource dataSource() {

        JdbcDataSource jdbcDataSource = new JdbcDataSource();
        jdbcDataSource.setURL(dbConnectionString);
        jdbcDataSource.setUser(username);
        jdbcDataSource.setPassword(password);
        return jdbcDataSource;
    }
}
