package com.westminster.ecommerceanalyzer.models;

public class AnalyzerException extends Exception {

    public AnalyzerException(String errorMessage) {
        super(errorMessage);
    }

    public AnalyzerException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }
}
