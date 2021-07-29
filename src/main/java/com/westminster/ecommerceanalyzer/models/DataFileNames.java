package com.westminster.ecommerceanalyzer.models;

public enum DataFileNames {
    PRODUCTS_FILE("products.csv", "products"),
    CUSTOMERS_FILE("customers.csv", "customers"),
    GEO_LOCATION_FILE("geo_locations.csv", "geo_locations"),
    ORDER_ITEMS_FILE("order_items.csv", "order_items"),
    ORDERS_FILE("orders.csv", "orders"),
    SELLER_FILE("sellers.csv", "sellers"),
    PAYMENTS_FILE("order_payments.csv", "payments");
    private final String fileName;
    private final String tableName;

    DataFileNames(String fileName, String tableName) {
        this.fileName = fileName;
        this.tableName = tableName;
    }

    public String getFileName() {
        return fileName;
    }

    public String getTableName() {
        return tableName;
    }
}
