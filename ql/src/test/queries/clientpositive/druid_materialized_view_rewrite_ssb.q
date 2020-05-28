--! qt:disabled:unstable; fails sometimes HIVE-23450
--! qt:dataset:part

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;

CREATE TABLE `customer_ext_n0`(
  `c_custkey` bigint, 
  `c_name` string, 
  `c_address` string, 
  `c_city` string, 
  `c_nation` string, 
  `c_region` string, 
  `c_phone` string, 
  `c_mktsegment` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/ssb/customer/' into table `customer_ext_n0`;

CREATE TABLE `customer_n0`(
  `c_custkey` bigint, 
  `c_name` string, 
  `c_address` string, 
  `c_city` string, 
  `c_nation` string, 
  `c_region` string, 
  `c_phone` string, 
  `c_mktsegment` string,
  primary key (`c_custkey`) disable rely)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

INSERT INTO `customer_n0`
SELECT * FROM `customer_ext_n0`;

CREATE TABLE `dates_ext_n0`(
  `d_datekey` bigint, 
  `d_date` string, 
  `d_dayofweek` string, 
  `d_month` string, 
  `d_year` int, 
  `d_yearmonthnum` int, 
  `d_yearmonth` string, 
  `d_daynuminweek` int,
  `d_daynuminmonth` int,
  `d_daynuminyear` int,
  `d_monthnuminyear` int,
  `d_weeknuminyear` int,
  `d_sellingseason` string,
  `d_lastdayinweekfl` int,
  `d_lastdayinmonthfl` int,
  `d_holidayfl` int ,
  `d_weekdayfl`int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/ssb/date/' into table `dates_ext_n0`;

CREATE TABLE `dates_n0`(
  `d_datekey` bigint, 
  `__time` timestamp, 
  `d_date` string, 
  `d_dayofweek` string, 
  `d_month` string, 
  `d_year` int, 
  `d_yearmonthnum` int, 
  `d_yearmonth` string, 
  `d_daynuminweek` int,
  `d_daynuminmonth` int,
  `d_daynuminyear` int,
  `d_monthnuminyear` int,
  `d_weeknuminyear` int,
  `d_sellingseason` string,
  `d_lastdayinweekfl` int,
  `d_lastdayinmonthfl` int,
  `d_holidayfl` int ,
  `d_weekdayfl`int,
  primary key (`d_datekey`) disable rely
)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

INSERT INTO `dates_n0`
SELECT
  `d_datekey`, 
  cast(`d_year` || '-' || `d_monthnuminyear` || '-' || `d_daynuminmonth` as timestamp), 
  `d_date`, 
  `d_dayofweek`, 
  `d_month`, 
  `d_year`, 
  `d_yearmonthnum`, 
  `d_yearmonth`, 
  `d_daynuminweek`,
  `d_daynuminmonth`,
  `d_daynuminyear`,
  `d_monthnuminyear`,
  `d_weeknuminyear`,
  `d_sellingseason`,
  `d_lastdayinweekfl`,
  `d_lastdayinmonthfl`,
  `d_holidayfl`,
  `d_weekdayfl`
FROM `dates_ext_n0`;

CREATE TABLE `ssb_part_ext_n0`(
  `p_partkey` bigint, 
  `p_name` string, 
  `p_mfgr` string, 
  `p_category` string, 
  `p_brand1` string, 
  `p_color` string, 
  `p_type` string, 
  `p_size` int, 
  `p_container` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/ssb/part/' into table `ssb_part_ext_n0`;

CREATE TABLE `ssb_part_n0`(
  `p_partkey` bigint, 
  `p_name` string, 
  `p_mfgr` string, 
  `p_category` string, 
  `p_brand1` string, 
  `p_color` string, 
  `p_type` string, 
  `p_size` int, 
  `p_container` string,
  primary key (`p_partkey`) disable rely)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

INSERT INTO `ssb_part_n0`
SELECT * FROM `ssb_part_ext_n0`;

CREATE TABLE `supplier_ext_n0`(
  `s_suppkey` bigint, 
  `s_name` string, 
  `s_address` string, 
  `s_city` string, 
  `s_nation` string, 
  `s_region` string, 
  `s_phone` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/ssb/supplier/' into table `supplier_ext_n0`;

CREATE TABLE `supplier_n0`(
  `s_suppkey` bigint, 
  `s_name` string, 
  `s_address` string, 
  `s_city` string, 
  `s_nation` string, 
  `s_region` string, 
  `s_phone` string,
  primary key (`s_suppkey`) disable rely)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

INSERT INTO `supplier_n0`
SELECT * FROM `supplier_ext_n0`;

CREATE TABLE `lineorder_ext_n0`(
  `lo_orderkey` bigint, 
  `lo_linenumber` int, 
  `lo_custkey` bigint not null disable rely,
  `lo_partkey` bigint not null disable rely,
  `lo_suppkey` bigint not null disable rely,
  `lo_orderdate` bigint not null disable rely,
  `lo_ordpriority` string, 
  `lo_shippriority` string, 
  `lo_quantity` double, 
  `lo_extendedprice` double, 
  `lo_ordtotalprice` double, 
  `lo_discount` double, 
  `lo_revenue` double, 
  `lo_supplycost` double, 
  `lo_tax` double, 
  `lo_commitdate` bigint, 
  `lo_shipmode` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/ssb/lineorder/' into table `lineorder_ext_n0`;

CREATE TABLE `lineorder_n0`(
  `lo_orderkey` bigint, 
  `lo_linenumber` int, 
  `lo_custkey` bigint not null disable rely,
  `lo_partkey` bigint not null disable rely,
  `lo_suppkey` bigint not null disable rely,
  `lo_orderdate` bigint not null disable rely,
  `lo_ordpriority` string, 
  `lo_shippriority` string, 
  `lo_quantity` double, 
  `lo_extendedprice` double, 
  `lo_ordtotalprice` double, 
  `lo_discount` double, 
  `lo_revenue` double, 
  `lo_supplycost` double, 
  `lo_tax` double, 
  `lo_commitdate` bigint, 
  `lo_shipmode` string,
  primary key (`lo_orderkey`) disable rely,
  constraint fk1 foreign key (`lo_custkey`) references `customer_n0`(`c_custkey`) disable rely,
  constraint fk2 foreign key (`lo_orderdate`) references `dates_n0`(`d_datekey`) disable rely,
  constraint fk3 foreign key (`lo_partkey`) references `ssb_part_n0`(`p_partkey`) disable rely,
  constraint fk4 foreign key (`lo_suppkey`) references `supplier_n0`(`s_suppkey`) disable rely)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

INSERT INTO `lineorder_n0`
SELECT * FROM `lineorder_ext_n0`;


-- CREATE MV
CREATE MATERIALIZED VIEW `ssb_mv_druid_100`
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "MONTH")
AS
SELECT
 `__time` as `__time` ,
 cast(c_city as string) c_city,
 cast(c_nation as string) c_nation,
 cast(c_region as string) c_region,
 c_mktsegment as c_mktsegment,
 cast(d_weeknuminyear as string) d_weeknuminyear,
 cast(d_year as string) d_year,
 cast(d_yearmonth as string) d_yearmonth,
 cast(d_yearmonthnum as string) d_yearmonthnum,
 cast(p_brand1 as string) p_brand1,
 cast(p_category as string) p_category,
 cast(p_mfgr as string) p_mfgr,
 p_type,
 s_name,
 cast(s_city as string) s_city,
 cast(s_nation as string) s_nation,
 cast(s_region as string) s_region,
 cast(`lo_ordpriority` as string) lo_ordpriority, 
 cast(`lo_shippriority` as string) lo_shippriority, 
 `d_sellingseason`
 `lo_shipmode`, 
 lo_revenue,
 lo_supplycost ,
 lo_discount ,
 `lo_quantity`, 
 `lo_extendedprice`, 
 `lo_ordtotalprice`, 
 lo_extendedprice * lo_discount discounted_price,
 lo_revenue - lo_supplycost net_revenue
FROM
 customer_n0, dates_n0, lineorder_n0, ssb_part_n0, supplier_n0
where
 lo_orderdate = d_datekey
 and lo_partkey = p_partkey
 and lo_suppkey = s_suppkey
 and lo_custkey = c_custkey;


-- QUERY OVER MV
EXPLAIN CBO
SELECT MONTH(`__time`) AS `mn___time_ok`,
CAST((MONTH(`__time`) - 1) / 3 + 1 AS BIGINT) AS `qr___time_ok`,
SUM(1) AS `sum_number_of_records_ok`,
YEAR(`__time`) AS `yr___time_ok`
FROM `ssb_mv_druid_100`
GROUP BY MONTH(`__time`),
CAST((MONTH(`__time`) - 1) / 3 + 1 AS BIGINT),
YEAR(`__time`);

SELECT MONTH(`__time`) AS `mn___time_ok`,
CAST((MONTH(`__time`) - 1) / 3 + 1 AS BIGINT) AS `qr___time_ok`,
SUM(1) AS `sum_number_of_records_ok`,
YEAR(`__time`) AS `yr___time_ok`
FROM `ssb_mv_druid_100`
GROUP BY MONTH(`__time`),
CAST((MONTH(`__time`) - 1) / 3 + 1 AS BIGINT),
YEAR(`__time`);


-- QUERY OVER ORIGINAL TABLES
EXPLAIN CBO
SELECT MONTH(`dates_n0`.`__time`) AS `mn___time_ok`,
CAST((MONTH(`dates_n0`.`__time`) - 1) / 3 + 1 AS BIGINT) AS `qr___time_ok`,
SUM(1) AS `sum_number_of_records_ok`,
YEAR(`dates_n0`.`__time`) AS `yr___time_ok`
FROM `lineorder_n0` `lineorder_n0`
JOIN `dates_n0` `dates_n0` ON (`lineorder_n0`.`lo_orderdate` = `dates_n0`.`d_datekey`)
JOIN `customer_n0` `customer_n0` ON (`lineorder_n0`.`lo_custkey` = `customer_n0`.`c_custkey`)
JOIN `supplier_n0` `supplier_n0` ON (`lineorder_n0`.`lo_suppkey` = `supplier_n0`.`s_suppkey`)
JOIN `ssb_part_n0` `ssb_part_n0` ON (`lineorder_n0`.`lo_partkey` = `ssb_part_n0`.`p_partkey`)
GROUP BY MONTH(`dates_n0`.`__time`),
CAST((MONTH(`dates_n0`.`__time`) - 1) / 3 + 1 AS BIGINT),
YEAR(`dates_n0`.`__time`);

SELECT MONTH(`dates_n0`.`__time`) AS `mn___time_ok`,
CAST((MONTH(`dates_n0`.`__time`) - 1) / 3 + 1 AS BIGINT) AS `qr___time_ok`,
SUM(1) AS `sum_number_of_records_ok`,
YEAR(`dates_n0`.`__time`) AS `yr___time_ok`
FROM `lineorder_n0` `lineorder_n0`
JOIN `dates_n0` `dates_n0` ON (`lineorder_n0`.`lo_orderdate` = `dates_n0`.`d_datekey`)
JOIN `customer_n0` `customer_n0` ON (`lineorder_n0`.`lo_custkey` = `customer_n0`.`c_custkey`)
JOIN `supplier_n0` `supplier_n0` ON (`lineorder_n0`.`lo_suppkey` = `supplier_n0`.`s_suppkey`)
JOIN `ssb_part_n0` `ssb_part_n0` ON (`lineorder_n0`.`lo_partkey` = `ssb_part_n0`.`p_partkey`)
GROUP BY MONTH(`dates_n0`.`__time`),
CAST((MONTH(`dates_n0`.`__time`) - 1) / 3 + 1 AS BIGINT),
YEAR(`dates_n0`.`__time`);

DROP MATERIALIZED VIEW `ssb_mv_druid_100`;
