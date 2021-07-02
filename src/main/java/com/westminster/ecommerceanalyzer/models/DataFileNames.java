package com.westminster.ecommerceanalyzer.models;

public enum DataFileNames {
    DATA_FILE_NAME_ONE("file-one", 1),
    DATA_FILE_NAME_TWO("file-two", 2);

    private final String name;
    private final int value;

    DataFileNames(String name1, int value) {
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
