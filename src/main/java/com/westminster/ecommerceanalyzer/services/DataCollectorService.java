package com.westminster.ecommerceanalyzer.services;

import com.westminster.ecommerceanalyzer.FileServerClient;
import com.westminster.ecommerceanalyzer.HiveConnector;
import com.westminster.ecommerceanalyzer.analyzers.AnalyzerRequest;
import com.westminster.ecommerceanalyzer.analyzers.WeeklyIncomeAnalyzer;
import com.westminster.ecommerceanalyzer.entities.DataRetrievalRepo;
import com.westminster.ecommerceanalyzer.entities.HiveQueryEntity;
import com.westminster.ecommerceanalyzer.entities.HiveQueryRepo;
import com.westminster.ecommerceanalyzer.models.AnalyzerNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

@Component
public class DataCollectorService {

    @Autowired
    private DataRetrievalRepo dataRetrievalRepo;
    @Autowired
    private HiveQueryRepo hiveQueryRepo;
    @Autowired
    private HiveConnector hiveConnector;
    @Autowired
    private WeeklyIncomeAnalyzer weeklyIncomeAnalyzer;
    @Autowired
    private FileServerClient fileServerClient;


    Logger logger = LoggerFactory.getLogger(DataCollectorService.class);

    @Scheduled(fixedDelay = 1000)
//    @Scheduled(cron="0 0 * * * SUN")
    @Async("taskExecutor")
    public void collect() throws SQLException, ClassNotFoundException {
        logger.info("starting the daily data collector.");
        List<Date> dates = dataRetrievalRepo.findLastSuccessfulDataRetrievalDate();
        List<String> directoryList;
        if (dates.isEmpty()) {
            directoryList = fileServerClient.getAllDirectories();
            System.out.println("hello");
        } else {
            directoryList = Collections.singletonList(dates.get(0).toString());
            logger.info("data to retrieve");
        }


        HiveQueryEntity query = hiveQueryRepo.findByName(AnalyzerNames.WEEKLY_INCOME_ANALYZER.getName());

        Connection con = hiveConnector.getConnection();
        String sql = "create table if not exists test1 (id int)";
        weeklyIncomeAnalyzer.run(new AnalyzerRequest(con, sql));
        hiveConnector.closeConnection(con);

    }

}
