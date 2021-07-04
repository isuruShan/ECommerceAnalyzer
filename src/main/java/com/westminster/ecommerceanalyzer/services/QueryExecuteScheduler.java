package com.westminster.ecommerceanalyzer.services;

import com.westminster.ecommerceanalyzer.HiveConnector;
import com.westminster.ecommerceanalyzer.entities.HiveQueryEntity;
import com.westminster.ecommerceanalyzer.entities.HiveQueryRepo;
import com.westminster.ecommerceanalyzer.models.HiveQueryNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Component
public class QueryExecuteScheduler {

    Logger logger = LoggerFactory.getLogger(QueryExecuteScheduler.class);
    @Autowired
    private HiveQueryRepo hiveQueryRepo;
    @Autowired
    private HiveConnector hiveConnector;

    //    @Scheduled(fixedDelay=60000)
    @Scheduled(cron = "0 0 * * * SUN")
    @Async("taskExecutor")
    public void executeWeeklyIncomeAnalyzer() throws SQLException, ClassNotFoundException {
        HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(HiveQueryNames.WEEKLY_INCOME_ANALYZER.getName(), true);
        logger.info("starting weekly income analyzer ");
        Connection con = hiveConnector.getConnection();
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery(query.getQuery());
        hiveConnector.closeConnection(con);
    }
}
