package com.westminster.ecommerceanalyzer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@Component
public class HdfsClient {

    @Autowired
    private Configuration hadoopConf;

    @Value("${hdfs.base.path}")
    private String hdfsBasePath;

    public void writeToDFS(String filePath, String data) throws IOException {
        InputStream dataStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        FileSystem fs = FileSystem.get(URI.create(hdfsBasePath), hadoopConf);
        FSDataOutputStream out = fs.create(new Path(filePath));
        IOUtils.copyBytes(dataStream, out, 4096, true);
    }

    public List<String> listDirectory(String directoryPath) throws IOException  {
        FileSystem fs = FileSystem.get(URI.create(hdfsBasePath), hadoopConf);
        FileStatus[] fileStatus = fs.listStatus(new Path(directoryPath));
        List<String> fileList = new ArrayList<>();
        for (FileStatus status : fileStatus) {
            fileList.add(status.getPath().toString());
        }
        return fileList;
    }

    public void deleteFile(String filePath) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsBasePath), hadoopConf);
        boolean out = fs.delete(new Path(filePath), false);
    }

    public void deleteDirectoryRecursive(String filePath) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsBasePath), hadoopConf);
        boolean out = fs.delete(new Path("/data/" + filePath), true);
    }
}
