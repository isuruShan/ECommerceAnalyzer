package com.westminster.ecommerceanalyzer.services;

import com.westminster.ecommerceanalyzer.FileServerClient;
import com.westminster.ecommerceanalyzer.entities.DataRetrievalEntity;
import com.westminster.ecommerceanalyzer.entities.DataRetrievalRepo;
import com.westminster.ecommerceanalyzer.models.CollectionStatus;
import com.westminster.ecommerceanalyzer.models.DataFileNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
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
    private Configuration hadoopConf;

    @Value("${hdfs.base.path}")
    private String hdfsBasePath;

    Logger logger = LoggerFactory.getLogger(DataCollectorService.class);

    @Scheduled(fixedDelay = 100000)
//    @Scheduled(cron="0 0 * * * SUN")
    @Async("taskExecutor")
    public void collect() throws SQLException, ClassNotFoundException {
        logger.info("starting the daily data collector.");
        List<Date> dates = dataRetrievalRepo.findLastSuccessfulDataRetrievalDate();
        DataRetrievalEntity dataRetrievalEntity = new DataRetrievalEntity();
        try {
            dataRetrievalEntity.setDate(new Date());
            dataRetrievalEntity.setStatus(CollectionStatus.IN_PROGRESS.getValue());
            dataRetrievalRepo.insert(dataRetrievalEntity);
            List<String> directoryList;
            if (dates.isEmpty()) {
                directoryList = fileServerClient.getAllDirectories();
            } else {
                directoryList = fileServerClient.getAllDirectoriesFromDate(dates.get(0));
            }
            directoryList.forEach(directory -> {
                Arrays.stream(DataFileNames.values()).forEach(file -> {
                    try {
                        writeToDFS(directory, file.getFileName(), fileServerClient.downloadFile(file, directory));
                        updateTable(file.getFileName(), directory);
                    } catch (SQLException | ClassNotFoundException | IOException throwables) {
                        throwables.printStackTrace();
                    }
                });
            });
            dataRetrievalEntity.setStatus(CollectionStatus.SUCCESSFUL.getValue());
            dataRetrievalRepo.insert(dataRetrievalEntity);
        } catch (Exception e) {
            dataRetrievalEntity.setStatus(CollectionStatus.FAILED.getValue());
            dataRetrievalRepo.insert(dataRetrievalEntity);
        }


    }

    private void updateTable(String fileName, String directory) throws SQLException, ClassNotFoundException {
        hiveTablesCreator.loadDataToTable(fileName, directory);
    }

    private void writeToDFS(String date, String fileName, String directory) throws IOException {
        InputStream dataStream = new ByteArrayInputStream(directory.getBytes(StandardCharsets.UTF_8));
        FileSystem fs = FileSystem.get(URI.create(hdfsBasePath + date), hadoopConf);
        FSDataOutputStream out = fs.create(new Path(fileName));
        FileStatus[] fileStatus = fs.listStatus(new Path("/user/"));
        for (FileStatus status : fileStatus) {
            System.out.println(status.getPath().toString());
        }
        IOUtils.copyBytes(dataStream, out, 4096, true);
    }
}
