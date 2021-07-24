--------------------- Start table create queries --------------------
CREATE TABLE IF NOT EXISTS orders(
    id                      String,
    customer_id             String,
    order_status            String,
    purchase_timestamp      timestamp,
    approved_at             timestamp,
    delivered_carrier_date  timestamp,
    delivered_customer_date timestamp,
    estimated_delivery_date timestamp
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties
(
    "skip.header.line.count"="1"
);

CREATE TABLE IF NOT EXISTS order_items
(
    order_id            int,
    id                  int,
    product_id          String,
    seller_id           String,
    shipping_limit_date timestamp,
    price               float,
    freight_value       float
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

CREATE TABLE IF NOT EXISTS geo_locations
(
    zip_code_prefix String,
    latitude        double,
    longitude       double,
    city            String,
    state           String
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS customers
(
    id              String,
    unique_id       String,
    zip_code_prefix String,
    city            String,
    state           String
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties
(
    "skip.header.line.count"="1"
);

CREATE TABLE IF NOT EXISTS payments
(
    order_id                String,
    payment_sequential      int,
    payment_type            String,
    payment_installments    int,
    payment_value           double
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties
(
    "skip.header.line.count"="1"
);

CREATE TABLE IF NOT EXISTS products( id String, category_name String, name_lenght int, description_lenght int, photos_qty int, weight_g int, length_cm int, height_cm int, width_cm int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties( "skip.header.line.count"="1" );
--------------------- End table create queries --------------------

--------------------- Start analyzer queries --------------------
SELECT count(*) from orders;
---- most sold products in least revenue generating states for a span of time ------
SELECT *
FROM (SELECT rank_phase.sales_per_product,
             rank_phase.product_id,
             rank_phase.state,
             rank() over ( PARTITION by rank_phase.state ORDER BY rank_phase.sales_per_product DESC ) as rank
      FROM (SELECT sum(oi.price) as sales_per_product, oi.product_id as product_id, c.state as state
            FROM order_items oi
                     INNER JOIN orders o on (oi.order_id = o.id)
                     INNER JOIN customers c on (c.id = o.customer_id)
            where c.state in (SELECT sps.state
                              FROM (SELECT sum(oi.price) as sales, c.state as state
                                    FROM order_items oi
                                             INNER JOIN orders o on (oi.order_id = o.id)
                                             INNER JOIN customers c on (o.customer_id = c.id)
                                    where purchase_timestamp BETWEEN unix_timestamp('2018-01-01 00:00:00.000', 'yyyy-MM-dd HH:mm:ss.SSS') AND unix_timestamp('2018-07-01 00:00:00.000', 'yyyy-MM-dd HH:mm:ss.SSS')
                                    GROUP BY c.state
                                    ORDER BY sales limit 5) sps)
            GROUP BY oi.product_id, c.state) rank_phase) final
WHERE final.rank <= 5
---- most sold products in least revenue generating states ------

---- daily sales analyzer ----



--------------------- End analyzer queries --------------------
