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

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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

    public List<String> listDirectory(String directoryPath) throws IOException {
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
        boolean out = fs.delete(new Path(filePath), true);
    }

    public void getFile(String filePath) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsBasePath), hadoopConf);
        InputStream in = null;
        try {
            in = fs.open(new Path(filePath));
            File targetFile = new File("/home/igayashan/targetFile.tmp");
            OutputStream outStream = new FileOutputStream(targetFile);

            byte[] buffer = new byte[8 * 1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }
            IOUtils.closeStream(in);
            org.apache.commons.io.IOUtils.closeQuietly(outStream);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
