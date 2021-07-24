package com.westminster.ecommerceanalyzer.services;

import com.westminster.ecommerceanalyzer.HdfsClient;
import com.westminster.ecommerceanalyzer.HiveConnector;
import com.westminster.ecommerceanalyzer.entities.AnalyzeJobEntity;
import com.westminster.ecommerceanalyzer.entities.AnalyzerJobRepo;
import com.westminster.ecommerceanalyzer.entities.HiveQueryEntity;
import com.westminster.ecommerceanalyzer.entities.HiveQueryRepo;
import com.westminster.ecommerceanalyzer.models.AnalyzerStatus;
import com.westminster.ecommerceanalyzer.models.HiveQueryNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.util.Date;

@Component
public class AnalyzersExecutorService {

    Logger logger = LoggerFactory.getLogger(AnalyzersExecutorService.class);
    @Autowired
    private HiveQueryRepo hiveQueryRepo;
    @Autowired
    private HiveConnector hiveConnector;
    @Autowired
    private HdfsClient hdfsClient;
    @Autowired
    AnalyzerJobRepo analyzerJobRepo;

    public static final String BASE_OUTPUT_FILE_PATH = "/data/output/";


    //    @Scheduled(fixedDelay=60000)
    @Scheduled(cron = "0 0 * * * SUN")
    @Async("taskExecutor")
    public void executeWeeklyIncomeAnalyzer() throws ParseException {
        AnalyzeJobEntity analyzeJobEntity = createRecordsForAnalyzer(HiveQueryNames.DAILY_INCOME_ANALYZER.getName());
        try {
            HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(HiveQueryNames.DAILY_INCOME_ANALYZER.getName(), true);
            logger.info("starting weekly income analyzer ");
            Connection con = hiveConnector.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(query.getQuery());
            String outputFilePath = BASE_OUTPUT_FILE_PATH + HiveQueryNames.MOST_REVENUE_LOCATIONS.getOutputFile();
            hdfsClient.deleteFile(outputFilePath);
            hdfsClient.writeToDFS(outputFilePath, resultSet.toString());
            hiveConnector.closeConnection(con);
            analyzeJobEntity.setStatus(AnalyzerStatus.SUCCESSFUL);
            analyzerJobRepo.save(analyzeJobEntity);
        } catch (Exception e) {
            analyzeJobEntity.setStatus(AnalyzerStatus.FAILED);
            analyzerJobRepo.save(analyzeJobEntity);
            e.printStackTrace();
        }

    }

    //    @Scheduled(fixedDelay=60000)
    @Scheduled(cron = "30 0 * * * SUN")
    @Async("taskExecutor")
    public void executeMostRevenueLocationsAnalyzer() throws SQLException, ClassNotFoundException, IOException, ParseException {
        AnalyzeJobEntity analyzeJobEntity = createRecordsForAnalyzer(HiveQueryNames.MOST_REVENUE_LOCATIONS.getName());
        try {
            HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(HiveQueryNames.MOST_REVENUE_LOCATIONS.getName(), true);
            logger.info("starting most revenue location analyzer ");
            Connection con = hiveConnector.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(query.getQuery());
            String outputFilePath = BASE_OUTPUT_FILE_PATH + HiveQueryNames.MOST_REVENUE_LOCATIONS.getOutputFile();
            hdfsClient.deleteFile(outputFilePath);
            hdfsClient.writeToDFS(outputFilePath, resultSet.toString());
            hiveConnector.closeConnection(con);
            analyzeJobEntity.setStatus(AnalyzerStatus.SUCCESSFUL);
            analyzerJobRepo.save(analyzeJobEntity);
        } catch (Exception e) {
            analyzeJobEntity.setStatus(AnalyzerStatus.FAILED);
            analyzerJobRepo.save(analyzeJobEntity);
            e.printStackTrace();
        }

    }

    //    @Scheduled(fixedDelay=60000)
    @Scheduled(fixedDelay = 600000, initialDelay = 5000)
    @Async("taskExecutor")
    public void executLeeastRevenueLocationsMostSellingProductsAnalyzer() throws ParseException {
        AnalyzeJobEntity analyzeJobEntity = createRecordsForAnalyzer(HiveQueryNames.LEAST_REVENUE_LOCATIONS_MOST_SELLING_PRODUCTS.getName());
        try {
            HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(HiveQueryNames.LEAST_REVENUE_LOCATIONS_MOST_SELLING_PRODUCTS.getName(), true);
            logger.info("starting most revenue location analyzer ");
            Connection con = hiveConnector.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(query.getQuery());
            String outputFilePath = BASE_OUTPUT_FILE_PATH + HiveQueryNames.LEAST_REVENUE_LOCATIONS_MOST_SELLING_PRODUCTS.getOutputFile();
            hdfsClient.deleteFile(outputFilePath);
            hdfsClient.writeToDFS(outputFilePath, resultSet.toString());
            hiveConnector.closeConnection(con);
            analyzeJobEntity.setStatus(AnalyzerStatus.SUCCESSFUL);
            analyzerJobRepo.save(analyzeJobEntity);
        } catch (Exception e) {
            analyzeJobEntity.setStatus(AnalyzerStatus.FAILED);
            analyzerJobRepo.save(analyzeJobEntity);
            e.printStackTrace();
        }
    }

    //    @Scheduled(fixedDelay=60000)
    @Scheduled(cron = "30 1 * * * SUN")
    @Async("taskExecutor")
    public void executeMostPopularSellersAnalyzer() throws ParseException {
        AnalyzeJobEntity analyzeJobEntity = createRecordsForAnalyzer(HiveQueryNames.MOST_POPULAR_SELLERS.getName());
        try {
            HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(HiveQueryNames.MOST_POPULAR_SELLERS.getName(), true);
            logger.info("starting most revenue location analyzer ");
            Connection con = hiveConnector.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(query.getQuery());
            String outputFilePath = BASE_OUTPUT_FILE_PATH + HiveQueryNames.MOST_POPULAR_SELLERS.getOutputFile();
            hdfsClient.deleteFile(outputFilePath);
            hdfsClient.writeToDFS(outputFilePath, resultSet.toString());
            hiveConnector.closeConnection(con);
            analyzeJobEntity.setStatus(AnalyzerStatus.SUCCESSFUL);
            analyzerJobRepo.save(analyzeJobEntity);
        } catch (Exception e) {
            analyzeJobEntity.setStatus(AnalyzerStatus.FAILED);
            analyzerJobRepo.save(analyzeJobEntity);
            e.printStackTrace();
        }
    }

    //    @Scheduled(fixedDelay=60000)
    @Scheduled(cron = "0 2 * * * SUN")
    @Async("taskExecutor")
    public void executMostSoldCreditcardProductsAnalyzer() throws ParseException {
        AnalyzeJobEntity analyzeJobEntity = createRecordsForAnalyzer(HiveQueryNames.MOST_SOLD_CREDIT_CARD_PRODUCTS.getName());
        try {
            HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(HiveQueryNames.MOST_SOLD_CREDIT_CARD_PRODUCTS.getName(), true);
            logger.info("starting most revenue location analyzer ");
            Connection con = hiveConnector.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(query.getQuery());
            String outputFilePath = BASE_OUTPUT_FILE_PATH + HiveQueryNames.MOST_SOLD_CREDIT_CARD_PRODUCTS.getOutputFile();
            hdfsClient.deleteFile(outputFilePath);
            hdfsClient.writeToDFS(outputFilePath, resultSet.toString());
            hiveConnector.closeConnection(con);
            analyzeJobEntity.setStatus(AnalyzerStatus.SUCCESSFUL);
            analyzerJobRepo.save(analyzeJobEntity);
        } catch (Exception e) {
            analyzeJobEntity.setStatus(AnalyzerStatus.FAILED);
            analyzerJobRepo.save(analyzeJobEntity);
            e.printStackTrace();
        }
    }


    private AnalyzeJobEntity createRecordsForAnalyzer(String analyzerName) throws ParseException {
        AnalyzeJobEntity previousAnalyzerJob = analyzerJobRepo.findAnalyzeJobEntityByAnalyzerNameAndLatestIsTrue(analyzerName);
        if (previousAnalyzerJob != null) {
            previousAnalyzerJob.setLatest(false);
            analyzerJobRepo.save(previousAnalyzerJob);
        }
        AnalyzeJobEntity analyzeJobEntityNew = new AnalyzeJobEntity();
        analyzeJobEntityNew.setAnalyzerName(analyzerName);
        analyzeJobEntityNew.setStatus(AnalyzerStatus.IN_PROGRESS);
        analyzeJobEntityNew.setLatest(true);
        analyzeJobEntityNew.setDateTriggered(new Date());
        analyzerJobRepo.save(analyzeJobEntityNew);
        return analyzeJobEntityNew;
    }
}
