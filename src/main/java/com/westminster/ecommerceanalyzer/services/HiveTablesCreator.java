package com.westminster.ecommerceanalyzer.services;

import com.westminster.ecommerceanalyzer.HiveConnector;
import com.westminster.ecommerceanalyzer.entities.HiveQueryEntity;
import com.westminster.ecommerceanalyzer.entities.HiveQueryRepo;
import com.westminster.ecommerceanalyzer.models.QueryParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

@Component
public class HiveTablesCreator {
    @Autowired
    private HiveQueryRepo hiveQueryRepo;
    @Autowired
    private HiveConnector hiveConnector;

    Logger logger = LoggerFactory.getLogger(HiveConnector.class);

    private void createTable(String tableName) throws SQLException, ClassNotFoundException {
        logger.info("starting create table " + tableName);
        HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(tableName, false);
        Connection con = hiveConnector.getConnection();
        String sql = "create table if not exists test1 (id int)";//      replace the query from the db to here
        Statement statement = con.createStatement();
        statement.execute(query.getQuery());
        hiveConnector.closeConnection(con);
    }

    public void loadDataToTable(String fileName,String directory) throws SQLException, ClassNotFoundException {
        logger.info("starting loading data to " + fileName + " table");
        createTable(fileName);
        HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(fileName, true);
        QueryParameters params = new QueryParameters();
        params.setParam("directory", directory);
        params.setParam("fileName", fileName);
        String queryWithParams = createQueryWithParams(params, query.getQuery());
        Connection con = hiveConnector.getConnection();
        Statement statement = con.createStatement();
        statement.execute(queryWithParams);
        hiveConnector.closeConnection(con);
    }

    private String createQueryWithParams(QueryParameters params, String query) {
        for(Map.Entry<String, String> param: params.getParameterMap().entrySet()) {
            query = query.replaceAll("\\$"+param.getKey(), param.getValue());
        }
        return query;
    }
}
