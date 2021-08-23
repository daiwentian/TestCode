package com.open.jp.notification.common;

import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.sql.DataSource;

@ApplicationScoped
public class DataSourceHolder {

    public static final String JNDI_NAME = "java:jboss/datasources/PostgresDS";

    @Resource(lookup = DataSourceHolder.JNDI_NAME)
    private DataSource dataSource;

    protected DataSourceHolder() {
    }

    public DataSourceHolder(DataSource dataSource) {
        if (dataSource == null) {
            throw new NullPointerException("dataSource is null");
        }
        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public String toString() {
        return "DataSourceHolder this=" + hashCode() + "{dataSource" + dataSource.hashCode() + "}";
    }

    public Connection newConnection() throws SQLException {
        Connection con = dataSource.getConnection();
//        con.setAutoCommit(false);
        return con;
    }
}

