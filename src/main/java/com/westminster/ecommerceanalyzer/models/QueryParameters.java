package com.westminster.ecommerceanalyzer.models;

import java.util.HashMap;
import java.util.Map;

public class QueryParameters {
    private Map<String, String> parameterMap;

    public QueryParameters() {
        this.parameterMap = new HashMap<>();
    }

    public void setParam(String name, String param) {
        this.parameterMap.put(name, param);
    }

    public Map<String, String> getParameterMap() {
        return this.parameterMap;
    }
}
