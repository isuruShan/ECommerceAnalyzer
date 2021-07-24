package com.westminster.ecommerceanalyzer.models;

public enum HiveQueryNames {
    PRODUCT_TABLE_CREATE("PRODUCT_TABLE_CREATE", "", 1),
    ORDER_TABLE_CREATE("ORDER_TABLE_CREATE", "", 2),
    ORDER_ITEM_TABLE_CREATE("ORDER_ITEM_TABLE_CREATE", "", 3),
    ORDER_PAYMENTS_TABLE_CREATE("ORDER_PAYMENTS_TABLE_CREATE", "", 4),
    CUSTOMER_TABLE_CREATE("CUSTOMER_TABLE_CREATE", "", 5),
    DAILY_INCOME_ANALYZER("DAILY_INCOME_ANALYZER", "daily-income-analyzer.csv", 6), //did a change in the file name and the concept of query for scenario 5
    MOST_REVENUE_LOCATIONS("MOST_REVENUE_LOCATIONS", "most-revenue-locations.csv", 7),
    LEAST_REVENUE_LOCATIONS_MOST_SELLING_PRODUCTS("LEAST_REVENUE_LOCATIONS_MOST_SELLING_PRODUCTS", "least-revenue-locations-most-selling-products.csv", 8),
    MOST_POPULAR_SELLERS("MOST_POPULAR_SELLERS", "most-popular-sellers.csv", 9),
    MOST_SOLD_CREDIT_CARD_PRODUCTS("MOST_SOLD_CREDIT_CARD_PRODUCTS", "most-sold-credit-card-products.csv", 10),
    LOAD_DATA_TO_TABLE("DATA_LOADER", "", 11);

    private final String name;
    private final String outputFile;
    private final int value;

    HiveQueryNames(String name, String outputFile, int value) {
        this.name = name;
        this.outputFile = outputFile;
        this.value = value;
    }

    public String getName() {
        return name;
    }
    public String getOutputFile() {
        return outputFile;
    }
    public int getValue() {
        return value;
    }
}
