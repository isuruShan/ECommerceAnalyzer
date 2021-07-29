package com.westminster.ecommerceanalyzer.models;

import org.codehaus.jackson.annotate.JsonProperty;

public class FileTransferDocuement {
    private String fileName;
    private String data;

    @JsonProperty("file-name")
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
