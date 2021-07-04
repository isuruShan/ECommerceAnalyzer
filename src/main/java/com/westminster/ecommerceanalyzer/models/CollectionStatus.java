package com.westminster.ecommerceanalyzer.models;

public enum CollectionStatus {
    SUCCESSFUL("SUCCESSFUL", 1),
    FAILED("FAILED", 2),
    IN_PROGRESS("IN_PROGRESS", 2);

    private final String name;
    private final int value;


    CollectionStatus(String name1, int value) {
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
