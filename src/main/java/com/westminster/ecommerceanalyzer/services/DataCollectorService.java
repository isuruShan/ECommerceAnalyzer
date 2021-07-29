package com.westminster.ecommerceanalyzer.services;

import com.westminster.ecommerceanalyzer.FileServerClient;
import com.westminster.ecommerceanalyzer.entities.DataRetrievalEntity;
import com.westminster.ecommerceanalyzer.entities.DataRetrievalRepo;
import com.westminster.ecommerceanalyzer.models.CollectionStatus;
import com.westminster.ecommerceanalyzer.models.DataFileNames;
import com.westminster.ecommerceanalyzer.models.FileTransferDocuement;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Component
public class DataCollectorService {

    @Autowired
    private DataRetrievalRepo dataRetrievalRepo;
    @Autowired
    private FileServerClient fileServerClient;
    @Autowired
    private HiveTablesCreator hiveTablesCreator;
    @Autowired
    RestTemplate restTemplate;

    @Value("${file-retriever-server-api.url}")
    private String fileServerApiUrl;

    public static final String BASE_INPUT_FILE_PATH = "/usr/local/data/";

    Logger logger = LoggerFactory.getLogger(DataCollectorService.class);

    @Scheduled(fixedDelay = 10000000)
//    @Scheduled(cron="0 0 * * * SUN")
    @Async("taskExecutor")
    public void collect() throws JSONException {
        logger.info("starting the daily data collector.");
        List<Date> dates = dataRetrievalRepo.findLastSuccessfulDataRetrievalDate();
        List<String> directoryList;
        if (dates.isEmpty()) {
            directoryList = fileServerClient.getAllDirectories();
        } else {
            directoryList = fileServerClient.getAllDirectoriesFromDate(dates.get(0));
        }
        if (directoryList.isEmpty()) {
            logger.info("There is no data to update");
        } else {
            DataRetrievalEntity dataRetrievalEntity = new DataRetrievalEntity();
            directoryList.forEach(directory -> {
                try {
                    dataRetrievalEntity.setDate(new SimpleDateFormat("yyyy-MM-dd").parse(directory));
                    dataRetrievalEntity.setStatus(CollectionStatus.IN_PROGRESS.getValue());
                    dataRetrievalRepo.insert(dataRetrievalEntity);
                    Arrays.stream(DataFileNames.values()).forEach(file -> {
                        try {
                            writeToRemoteServer(file.getFileName(), fileServerClient.downloadFile(file, directory));
                            updateTable(file, directory);
                        } catch (SQLException | ClassNotFoundException e) {
                            dataRetrievalEntity.setStatus(CollectionStatus.FAILED.getValue());
                            dataRetrievalRepo.update(dataRetrievalEntity);
                            logger.info("Data retrieval process interrupted.");
                            e.printStackTrace();
                        }
                    });
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            });
            dataRetrievalEntity.setStatus(CollectionStatus.SUCCESSFUL.getValue());
            dataRetrievalRepo.update(dataRetrievalEntity);
            logger.info("Completed data retrieval process successfully.");
        }

    }

    private void updateTable(DataFileNames fileName, String directory) throws SQLException, ClassNotFoundException {
        hiveTablesCreator.loadDataToTable(fileName, directory);
    }

    private void writeToRemoteServer(String fileName, String data) {
        FileTransferDocuement fileTransferDocuement = new FileTransferDocuement();
        fileTransferDocuement.setData(data);
        fileTransferDocuement.setFileName(fileName);
        restTemplate.postForObject(URI.create(fileServerApiUrl + "store-file"), fileTransferDocuement, String.class);
    }
}
