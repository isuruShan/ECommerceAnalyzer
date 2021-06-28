package com.westminster.ecommerceanalyzer.services;

import com.westminster.ecommerceanalyzer.HiveConnector;
import com.westminster.ecommerceanalyzer.analyzers.AnalyzerRequest;
import com.westminster.ecommerceanalyzer.analyzers.WeeklyIncomeAnalyzer;
import com.westminster.ecommerceanalyzer.entities.HiveQueryEntity;
import com.westminster.ecommerceanalyzer.entities.HiveQueryRepo;
import com.westminster.ecommerceanalyzer.models.AnalyzerNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.*;

@Component
public class QueryExecutorService {

    Logger logger = LoggerFactory.getLogger(QueryExecutorService.class);
    @Autowired
    private HiveQueryRepo hiveQueryRepo;
    @Autowired
    private HiveConnector hiveConnector;
    @Autowired
    private WeeklyIncomeAnalyzer weeklyIncomeAnalyzer;


    @Scheduled(fixedDelay=60000)
//    @Scheduled(cron="0 0 * * SUN")
    @Async("taskExecutor")
    public void executeWeeklyIncomeAnalyzer() throws SQLException, ClassNotFoundException {
        HiveQueryEntity query = hiveQueryRepo.findByName(AnalyzerNames.WEEKLY_INCOME_ANALYZER.getName());
        logger.info("starting weekly income analyzer ");
        Connection con = hiveConnector.getConnection();
        String sql = "SELECT * from ratings";
        weeklyIncomeAnalyzer.run(new AnalyzerRequest(con, sql));
        hiveConnector.closeConnection(con);
    }
}
