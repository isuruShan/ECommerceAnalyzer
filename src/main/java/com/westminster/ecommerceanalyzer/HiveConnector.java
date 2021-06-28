package com.westminster.ecommerceanalyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Component
public class HiveConnector {

    Logger logger = LoggerFactory.getLogger(HiveConnector.class);
    private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    @Value("${hive.database.uri}")
    private String hiveDbUri;
    @Value("${hive.database.username}")
    private String hiveDbUserName;
    @Value("${hive.database.password}")
    private String hiveDbPassword;

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        logger.info("starting new hive connection");
        Class.forName(DRIVER_NAME);
        return DriverManager.getConnection(hiveDbUri, hiveDbUserName, hiveDbPassword );
    }
    public void closeConnection(Connection con) throws SQLException {
        con.close();
        logger.info("closed the hive connection");
    }

}
