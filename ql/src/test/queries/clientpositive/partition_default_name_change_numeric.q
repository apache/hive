set hive.auto.convert.join=true;

DROP TABLE IF EXISTS sales_p_int;
CREATE EXTERNAL TABLE sales_p_int (ss_quantity INT, ss_sales_price DECIMAL(7,2)) PARTITIONED BY (ss_sold_date_sk_int INT) STORED AS ORC;

DROP TABLE IF EXISTS sales_p_bigint;
CREATE EXTERNAL TABLE sales_p_bigint (ss_quantity INT, ss_sales_price DECIMAL(7,2)) PARTITIONED BY (ss_sold_date_sk_bigint BIGINT) STORED AS ORC;

DROP TABLE IF EXISTS sales_p_double;
CREATE EXTERNAL TABLE sales_p_double (ss_quantity INT, ss_sales_price DECIMAL(7,2)) PARTITIONED BY (ss_sold_date_sk_double DOUBLE) STORED AS ORC;

DROP TABLE IF EXISTS sales_p_decimal;
CREATE EXTERNAL TABLE sales_p_decimal (ss_quantity INT, ss_sales_price DECIMAL(7,2)) PARTITIONED BY (ss_sold_date_sk_decimal DECIMAL(10,2)) STORED AS ORC;

DROP TABLE IF EXISTS date_dim_multi;
CREATE EXTERNAL TABLE date_dim_multi (
    d_date_sk_int INT,
    d_date_sk_bigint BIGINT,
    d_date_sk_double DOUBLE,
    d_date_sk_decimal DECIMAL(10,2),
    d_date DATE,
    d_year INT
) STORED AS ORC;

INSERT INTO sales_p_int PARTITION (ss_sold_date_sk_int) VALUES (1, 9.99, 24518), (2, 5.55, null);
INSERT INTO sales_p_bigint PARTITION (ss_sold_date_sk_bigint) VALUES (1, 9.99, 2451800001), (2, 5.55, null);
INSERT INTO sales_p_double PARTITION (ss_sold_date_sk_double) VALUES (1, 9.99, 24518.01), (2, 5.55, null);
INSERT INTO sales_p_decimal PARTITION (ss_sold_date_sk_decimal) VALUES (1, 9.99, 24518.01), (2, 5.55, null);

INSERT INTO date_dim_multi VALUES
  (24518, 2451800001, 24518.01, 24518.01, '2020-01-01', 2020),
  (24519, 2451900002, 24519.02, 24519.02, '2020-01-02', 2020);

-- modify hive default partition name post insertion
alter table sales_p_int set default partition to 'abc';
alter table sales_p_bigint set default partition to 'abc';
alter table sales_p_double set default partition to 'abc';
alter table sales_p_decimal set default partition to 'abc';


SELECT d_date FROM sales_p_int s, date_dim_multi d WHERE s.ss_sold_date_sk_int = d.d_date_sk_int and d.d_year = 2020 GROUP BY d_date;
SELECT d_date FROM sales_p_bigint s JOIN date_dim_multi d ON s.ss_sold_date_sk_bigint = d.d_date_sk_bigint WHERE d.d_year = 2020 GROUP BY d_date;
SELECT d_date FROM sales_p_double s JOIN date_dim_multi d ON s.ss_sold_date_sk_double = d.d_date_sk_double WHERE d.d_year = 2020 GROUP BY d_date;
set hive.vectorized.execution.enabled=false;
SELECT d_date FROM sales_p_decimal s JOIN date_dim_multi d ON s.ss_sold_date_sk_decimal = d.d_date_sk_decimal WHERE d.d_year = 2020 GROUP BY d_date;
