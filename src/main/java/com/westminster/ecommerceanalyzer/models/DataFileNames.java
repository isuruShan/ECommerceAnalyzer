package com.westminster.ecommerceanalyzer.models;

public enum DataFileNames {
    CUSTOMERS_FILE("customers", "customers"),
    GEO_LOCATION_FILE("geo_locations", "geo_locations"),
    SELLERS_FILE("sellers", "sellers");

    private final String name;
    private final String tableName;

    DataFileNames(String name1, String value) {
        this.name = name1;
        this.tableName = value;
    }

    public String getName() {
        return name;
    }

    public String getTableName() {
        return tableName;
    }
}
