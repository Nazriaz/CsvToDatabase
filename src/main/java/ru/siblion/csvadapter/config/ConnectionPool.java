package ru.siblion.csvadapter.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPool {
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;

    public static void configure(String dbConnectionString, String username, String password){
        config.setJdbcUrl( dbConnectionString );
        config.setUsername( username );
        config.setPassword( password );
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        ds = new HikariDataSource( config );
    }

    private ConnectionPool() {}

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}