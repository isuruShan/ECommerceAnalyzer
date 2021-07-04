package com.westminster.ecommerceanalyzer.services;

import com.westminster.ecommerceanalyzer.FileServerClient;
import com.westminster.ecommerceanalyzer.entities.DataRetrievalRepo;
import com.westminster.ecommerceanalyzer.entities.HiveQueryRepo;
import com.westminster.ecommerceanalyzer.models.DataFileNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Component
public class DataCollectorService {

    @Autowired
    private DataRetrievalRepo dataRetrievalRepo;
    @Autowired
    private HiveQueryRepo hiveQueryRepo;
    @Autowired
    private FileServerClient fileServerClient;
    @Autowired
    private HiveTablesCreator hiveTablesCreator;

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
        } else {
            directoryList = fileServerClient.getAllDirectoriesFromDate(dates.get(0));
        }
        directoryList.forEach(directory -> {
            Arrays.stream(DataFileNames.values()).forEach(file -> {
                try {
                    updateTable(file, fileServerClient.downloadFile(file, directory));
                } catch (SQLException | ClassNotFoundException throwables) {
                    throwables.printStackTrace();
                }
            });
        });

    }

    private void updateTable(DataFileNames fileName, String data) throws SQLException, ClassNotFoundException {
        hiveTablesCreator.loadDataToTable(fileName, data);
    }
}
