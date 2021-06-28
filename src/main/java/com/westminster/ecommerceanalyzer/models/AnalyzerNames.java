package com.westminster.ecommerceanalyzer.models;

public enum AnalyzerNames {
    WEEKLY_INCOME_ANALYZER("WEEKLY_INCOME_ANALYZER", 1),
    DAILY_INCOME_ANALYZER("DAILY_INCOME_ANALYZER", 2);

    private final String name;
    private final int value;

    AnalyzerNames(String name1, int value) {
        this.name = name1;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public int getValue() {
        return value;
    }
}
