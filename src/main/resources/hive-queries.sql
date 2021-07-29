--------------------- Start table create queries --------------------
CREATE TABLE IF NOT EXISTS orders
(
    id
    String,
    customer_id
    String,
    order_status
    String,
    purchase_timestamp
    timestamp,
    approved_at
    timestamp,
    delivered_carrier_date
    timestamp,
    delivered_customer_date
    timestamp,
    estimated_delivery_date
    timestamp
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties
(
    "skip.header.line.count"="1"
);

CREATE TABLE IF NOT EXISTS order_items
(
    order_id
    String,
    id
    int,
    product_id
    String,
    seller_id
    String,
    shipping_limit_date
    timestamp,
    price
    float,
    freight_value
    float
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties
(
    "skip.header.line.count"="1"
);

CREATE TABLE IF NOT EXISTS geo_locations
(
    zip_code_prefix
    String,
    latitude
    double,
    longitude
    double,
    city
    String,
    state
    String
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties
(
    "skip.header.line.count"="1"
);

CREATE TABLE IF NOT EXISTS sellers
(
    id
    String,
    zip_code_prefix
    String,
    city
    String,
    state
    String
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties
(
    "skip.header.line.count"="1"
);

CREATE TABLE IF NOT EXISTS customers
(
    id
    String,
    unique_id
    String,
    zip_code_prefix
    String,
    city
    String,
    state
    String
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties
(
    "skip.header.line.count"="1"
);

CREATE TABLE IF NOT EXISTS payments
(
    order_id
    String,
    payment_sequential
    int,
    payment_type
    String,
    payment_installments
    int,
    payment_value
    double
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties
(
    "skip.header.line.count"="1"
);

CREATE TABLE IF NOT EXISTS products
(
    id
    String,
    category_name
    String,
    name_lenght
    int,
    description_lenght
    int,
    photos_qty
    int,
    weight_g
    int,
    length_cm
    int,
    height_cm
    int,
    width_cm
    int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' tblproperties
(
    "skip.header.line.count"="1"
);
--------------------- End table create queries --------------------

--------------------- Start analyzer queries --------------------

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
WHERE final.rank <= 5;
---- most sold products in least revenue generating states ------

---- daily sales analyzer ----
SELECT * from (
select qlf.product,
       qlf.category,
       qlf.sales_percentage,
       qlf.sales_amout,
       rank() over ( PARTITION by qlf.category ORDER BY qlf.sales_percentage DESC ) as rank
from (
         select p.id                                     as product,
                ql.category                              as category,
                (sum(oi.price) * 100 / ql.category_sale) as sales_percentage,
                sum(oi.price)                            as sales_amout

         from order_items oi
                  inner join products p on (p.id = oi.product_id)
                  inner join (select sum(oi1.price) as category_sale, p1.category_name as category
                              from order_items oi1
                                       inner join products p1 on (p1.id = oi1.product_id)
                              group by p1.category_name) ql on (ql.category = p.category_name)
         group by p.id, ql.category_sale, ql.category) qlf) final
where final.rank < 10;
--------------------- End analyzer queries --------------------

--- most revenue generation geo location ---
select c.city, sum(py.payment_value) as salesfrom orders o
         inner join payments py
                    on (o.id = py.order_id)
         inner join customers c on (o.customer_id = c.id)
group by c.city
order by sales desc limit 10;

--- most revenue generating sellers ----
select sales_per_seller, seller_id, product
from (
         select ql.sales_per_seller, ql.seller_id, ql.product,
                rank() over ( PARTITION by ql.product ORDER BY ql.sales_per_seller DESC ) as rank
         from (
                  select sum(oi.price) as sales_per_seller, s.id as seller_id, oi.product_id as product
                  from order_items oi
                           inner join sellers s on (oi.seller_id = s.id)
                  group by s.id, oi.product_id) ql) final
where final.rank <= 1;
