package com.westminster.ecommerceanalyzer;

import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Component
public class FileServerClient {

    @Autowired
    RestTemplate restTemplate;
    @Value("${apache.url}")
    private String apacheUrl;
    @Value("${file-server-api.url}")
    private String fileServerApiUrl;

    public List<String> getAllDirectories() {
        String result = restTemplate.getForObject(fileServerApiUrl + "get-all-files", String.class);
        JSONArray jsonArray = new JSONArray(result);
        List<String> listdata = new ArrayList<String>();
        if (jsonArray.length() > 0 ) {
            for (int i=0;i<jsonArray.length();i++){
                listdata.add((String)jsonArray.get(i));
            }
        }
        return listdata;
    }
    public List<String> getAllDirectoriesFromDate(Date fromDate) {
        String result = restTemplate.getForObject(fileServerApiUrl + "hello.py", String.class);
        JSONArray jsonArray = new JSONArray(result);
        List<String> listdata = new ArrayList<String>();
        if (jsonArray.length() > 0 ) {
            for (int i=0;i<jsonArray.length();i++){
                listdata.add((String)jsonArray.get(i));
            }
        }
        return listdata;
    }
}
