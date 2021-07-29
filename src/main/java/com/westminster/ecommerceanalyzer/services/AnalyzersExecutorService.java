package com.westminster.ecommerceanalyzer.services;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.westminster.ecommerceanalyzer.HdfsClient;
import com.westminster.ecommerceanalyzer.HiveConnector;
import com.westminster.ecommerceanalyzer.entities.AnalyzeJobEntity;
import com.westminster.ecommerceanalyzer.entities.AnalyzerJobRepo;
import com.westminster.ecommerceanalyzer.entities.HiveQueryEntity;
import com.westminster.ecommerceanalyzer.entities.HiveQueryRepo;
import com.westminster.ecommerceanalyzer.models.AnalyzerException;
import com.westminster.ecommerceanalyzer.models.AnalyzerStatus;
import com.westminster.ecommerceanalyzer.models.HiveQueryNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
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
    @Autowired
    RestTemplate restTemplate;

    @Value("${file-retriever-server-api.url}")
    private String fileServerApiUrl;
    @Value("${file-retriever-file-server-api.url}")
    private String fileRetrievalFileServer;

    public static final String BASE_OUTPUT_FILE_PATH = "/data/output/";
    public static final String DEFAULT_OUTPUT_FILE_PATH = "/000000_0";
    public static final String STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;" +
            "AccountName=dmscsa01;" +
            "AccountKey=hNx2kklIvZyxqCdIpQLUHpYt1D9dc0xx5bfY5E/1pvkIUAkoCzJ58L7RLmwM2Hkk3AIZm0+JJkihr8jeGpRzjA==;" +
            "EndpointSuffix=core.windows.net";
    public static final String BLOB_CONTAINER = "olist-analyzer-results";


    @Scheduled(cron = "0 0 * * * SUN")
    @Async("taskExecutor")
    public void executeWeeklyIncomeAnalyzer() throws ParseException, AnalyzerException {
        AnalyzeJobEntity analyzeJobEntity = createRecordsForAnalyzer(HiveQueryNames.DAILY_INCOME_ANALYZER.getName());
        try {
            HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(HiveQueryNames.DAILY_INCOME_ANALYZER.getName(), true);
            logger.info("starting weekly income analyzer ");
            Connection con = hiveConnector.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(query.getQuery());
            statement.execute("INSERT OVERWRITE DIRECTORY '/data/output/" + HiveQueryNames.MOST_REVENUE_LOCATIONS.getOutputFile() + "' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " + query.getQuery());
            String fileServerFilePath = putFilesFromTheHDFSToLocal(HiveQueryNames.MOST_REVENUE_LOCATIONS.getOutputFile());
            putFilesToBlobStore(fileServerFilePath, HiveQueryNames.MOST_REVENUE_LOCATIONS.getOutputFile());
            hiveConnector.closeConnection(con);
            analyzeJobEntity.setStatus(AnalyzerStatus.SUCCESSFUL);
            analyzerJobRepo.save(analyzeJobEntity);
        } catch (Exception e) {
            analyzeJobEntity.setStatus(AnalyzerStatus.FAILED);
            analyzerJobRepo.save(analyzeJobEntity);
            throw new AnalyzerException("Analyzer job Exception", e);
        }

    }

    @Scheduled(cron = "30 0 * * * SUN")
    @Async("taskExecutor")
    public void executeMostRevenueLocationsAnalyzer() throws ParseException, AnalyzerException {
        AnalyzeJobEntity analyzeJobEntity = createRecordsForAnalyzer(HiveQueryNames.MOST_REVENUE_LOCATIONS.getName());
        try {
            HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(HiveQueryNames.MOST_REVENUE_LOCATIONS.getName(), true);
            logger.info("starting most revenue location analyzer ");
            Connection con = hiveConnector.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(query.getQuery());
            statement.execute("INSERT OVERWRITE DIRECTORY '/data/output/" + HiveQueryNames.MOST_REVENUE_LOCATIONS.getOutputFile() + "' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " + query.getQuery());
            String fileServerFilePath = putFilesFromTheHDFSToLocal(HiveQueryNames.MOST_REVENUE_LOCATIONS.getOutputFile());
            putFilesToBlobStore(fileServerFilePath, HiveQueryNames.MOST_REVENUE_LOCATIONS.getOutputFile());
            hiveConnector.closeConnection(con);
            analyzeJobEntity.setStatus(AnalyzerStatus.SUCCESSFUL);
            analyzerJobRepo.save(analyzeJobEntity);
        } catch (Exception e) {
            analyzeJobEntity.setStatus(AnalyzerStatus.FAILED);
            analyzerJobRepo.save(analyzeJobEntity);
            throw new AnalyzerException("Analyzer job Exception", e);
        }

    }

    @Scheduled(cron = "30 0 * * * SUN")
//    @Scheduled(fixedDelay = 600000, initialDelay = 5000)
    @Async("taskExecutor")
    public void executeLeastRevenueLocationsMostSellingProductsAnalyzer() throws ParseException, AnalyzerException {
        AnalyzeJobEntity analyzeJobEntity = createRecordsForAnalyzer(HiveQueryNames.LEAST_REVENUE_LOCATIONS_MOST_SELLING_PRODUCTS.getName());
        try {
            HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(HiveQueryNames.LEAST_REVENUE_LOCATIONS_MOST_SELLING_PRODUCTS.getName(), true);
            logger.info("starting most revenue location analyzer ");
            Connection con = hiveConnector.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(query.getQuery());
            statement.execute("INSERT OVERWRITE DIRECTORY '/data/output/" + HiveQueryNames.LEAST_REVENUE_LOCATIONS_MOST_SELLING_PRODUCTS.getOutputFile() + "' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " + query.getQuery());
            String fileServerFilePath = putFilesFromTheHDFSToLocal(HiveQueryNames.LEAST_REVENUE_LOCATIONS_MOST_SELLING_PRODUCTS.getOutputFile());
            putFilesToBlobStore(fileServerFilePath, HiveQueryNames.LEAST_REVENUE_LOCATIONS_MOST_SELLING_PRODUCTS.getOutputFile());
            hiveConnector.closeConnection(con);
            analyzeJobEntity.setStatus(AnalyzerStatus.SUCCESSFUL);
            analyzerJobRepo.save(analyzeJobEntity);
        } catch (Exception e) {
            analyzeJobEntity.setStatus(AnalyzerStatus.FAILED);
            analyzerJobRepo.save(analyzeJobEntity);
            throw new AnalyzerException("Analyzer job Exception", e);
        }
    }

    //    @Scheduled(fixedDelay = 600000, initialDelay = 5000)
    @Scheduled(cron = "30 1 * * * SUN")
    @Async("taskExecutor")
    public void executeMostPopularSellersAnalyzer() throws ParseException, AnalyzerException {
        AnalyzeJobEntity analyzeJobEntity = createRecordsForAnalyzer(HiveQueryNames.MOST_POPULAR_SELLERS.getName());
        try {
            HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(HiveQueryNames.MOST_POPULAR_SELLERS.getName(), true);
            logger.info("starting most revenue location analyzer ");
            Connection con = hiveConnector.getConnection();
            Statement statement = con.createStatement();
            statement.execute("INSERT OVERWRITE DIRECTORY '/data/output/" + HiveQueryNames.MOST_POPULAR_SELLERS.getOutputFile() + "' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " + query.getQuery());
            String fileServerFilePath = putFilesFromTheHDFSToLocal(HiveQueryNames.MOST_POPULAR_SELLERS.getOutputFile());
            putFilesToBlobStore(fileServerFilePath, HiveQueryNames.MOST_POPULAR_SELLERS.getOutputFile());
            hiveConnector.closeConnection(con);
            analyzeJobEntity.setStatus(AnalyzerStatus.SUCCESSFUL);
            analyzerJobRepo.save(analyzeJobEntity);
        } catch (Exception e) {
            analyzeJobEntity.setStatus(AnalyzerStatus.FAILED);
            analyzerJobRepo.save(analyzeJobEntity);
            throw new AnalyzerException("Analyzer job Exception", e);
        }
    }

    //    @Scheduled(fixedDelay=60000)
    @Scheduled(cron = "0 2 * * * SUN")
    @Async("taskExecutor")
    public void executeMostSoldCreditCardProductsAnalyzer() throws ParseException, AnalyzerException {
        AnalyzeJobEntity analyzeJobEntity = createRecordsForAnalyzer(HiveQueryNames.MOST_SOLD_CREDIT_CARD_PRODUCTS.getName());
        try {
            HiveQueryEntity query = hiveQueryRepo.findByNameAndAndDML(HiveQueryNames.MOST_SOLD_CREDIT_CARD_PRODUCTS.getName(), true);
            logger.info("starting most revenue location analyzer ");
            Connection con = hiveConnector.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(query.getQuery());
            statement.execute("INSERT OVERWRITE DIRECTORY '/data/output/" + HiveQueryNames.MOST_SOLD_CREDIT_CARD_PRODUCTS.getOutputFile() + "' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " + query.getQuery());
            String fileServerFilePath = putFilesFromTheHDFSToLocal(HiveQueryNames.MOST_SOLD_CREDIT_CARD_PRODUCTS.getOutputFile());
            putFilesToBlobStore(fileServerFilePath, HiveQueryNames.MOST_SOLD_CREDIT_CARD_PRODUCTS.getOutputFile());
            hiveConnector.closeConnection(con);
            analyzeJobEntity.setStatus(AnalyzerStatus.SUCCESSFUL);
            analyzerJobRepo.save(analyzeJobEntity);
        } catch (Exception e) {
            analyzeJobEntity.setStatus(AnalyzerStatus.FAILED);
            analyzerJobRepo.save(analyzeJobEntity);
            throw new AnalyzerException("Analyzer job Exception", e);
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

    private String putFilesFromTheHDFSToLocal(String fileName) {
        return restTemplate.getForObject(URI.create(fileServerApiUrl + "get-file?fileName=" + fileName), String.class);
    }

    private void putFilesToBlobStore(String filePath, String fileName) throws URISyntaxException, StorageException, InvalidKeyException, IOException {
        String result = restTemplate.getForObject(fileRetrievalFileServer + filePath, String.class);
        InputStream dataStream = new ByteArrayInputStream(result.getBytes(StandardCharsets.UTF_8));
        // Parse the connection string and create a blob client to interact with Blob storage
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(STORAGE_CONNECTION_STRING);
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        CloudBlobContainer container = blobClient.getContainerReference(BLOB_CONTAINER);
        CloudBlockBlob blob = container.getBlockBlobReference(fileName);
        blob.upload(dataStream, result.length());
    }
}
