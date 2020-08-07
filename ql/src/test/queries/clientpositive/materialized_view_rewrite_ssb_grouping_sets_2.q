--! qt:dataset:part

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;
set hive.stats.column.autogather=true;

CREATE TABLE `customer_ext_n4`(
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

LOAD DATA LOCAL INPATH '../../data/files/ssb/customer/' into table `customer_ext_n4`;

CREATE TABLE `customer_n4`(
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

INSERT INTO `customer_n4`
SELECT * FROM `customer_ext_n4`;

CREATE TABLE `dates_ext_n4`(
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

LOAD DATA LOCAL INPATH '../../data/files/ssb/date/' into table `dates_ext_n4`;

CREATE TABLE `dates_n4`(
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
  `d_weekdayfl`int,
  primary key (`d_datekey`) disable rely
)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

INSERT INTO `dates_n4`
SELECT * FROM `dates_ext_n4`;

CREATE TABLE `ssb_part_ext_n4`(
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

LOAD DATA LOCAL INPATH '../../data/files/ssb/part/' into table `ssb_part_ext_n4`;

CREATE TABLE `ssb_part_n4`(
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

INSERT INTO `ssb_part_n4`
SELECT * FROM `ssb_part_ext_n4`;

CREATE TABLE `supplier_ext_n4`(
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

LOAD DATA LOCAL INPATH '../../data/files/ssb/supplier/' into table `supplier_ext_n4`;

CREATE TABLE `supplier_n4`(
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

INSERT INTO `supplier_n4`
SELECT * FROM `supplier_ext_n4`;

CREATE TABLE `lineorder_ext_n4`(
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

LOAD DATA LOCAL INPATH '../../data/files/ssb/lineorder/' into table `lineorder_ext_n4`;

CREATE TABLE `lineorder_n4`(
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
  constraint fk1 foreign key (`lo_custkey`) references `customer_n4`(`c_custkey`) disable rely,
  constraint fk2 foreign key (`lo_orderdate`) references `dates_n4`(`d_datekey`) disable rely,
  constraint fk3 foreign key (`lo_partkey`) references `ssb_part_n4`(`p_partkey`) disable rely,
  constraint fk4 foreign key (`lo_suppkey`) references `supplier_n4`(`s_suppkey`) disable rely)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

INSERT INTO `lineorder_n4`
SELECT * FROM `lineorder_ext_n4`;


CREATE MATERIALIZED VIEW `ssb_mv_n4`
AS
SELECT
  p_brand1,
  p_category,
  s_city,
  s_nation,
  s_region,
  d_yearmonthnum,
  d_yearmonth,
  d_year,
  GROUPING__ID,
  sum(lo_revenue),
  sum(lo_extendedprice * lo_discount) discounted_price,
  sum(lo_revenue - lo_supplycost) net_revenue
FROM
  customer_n4, dates_n4, lineorder_n4, ssb_part_n4, supplier_n4
WHERE
  lo_orderdate = d_datekey
  and lo_partkey = p_partkey
  and lo_suppkey = s_suppkey
  and lo_custkey = c_custkey
GROUP BY
  d_year,
  d_yearmonth,
  d_yearmonthnum,
  s_region,
  s_nation,
  s_city,
  p_category,
  p_brand1
WITH ROLLUP;


-- Q2.1
explain cbo
select 
    sum(lo_revenue) as lo_revenue, d_year, p_brand1
from 
    lineorder_n4, dates_n4, ssb_part_n4, supplier_n4
where 
    lo_orderdate = d_datekey
    and lo_partkey = p_partkey
    and lo_suppkey = s_suppkey
    and p_category = 'MFGR#12'
    and s_region = 'AMERICA'
group by 
    d_year, p_brand1
order by 
    d_year, p_brand1;

-- Q2.2
explain cbo
select 
    sum(lo_revenue) as lo_revenue, d_year, p_brand1
from 
    lineorder_n4, dates_n4, ssb_part_n4, supplier_n4
where 
    lo_orderdate = d_datekey
    and lo_partkey = p_partkey
    and lo_suppkey = s_suppkey
    and p_brand1 between 'MFGR#2221' and 'MFGR#2228'
    and s_region = 'ASIA'
group by 
    d_year, p_brand1
order by 
    d_year, p_brand1;

-- Q2.3
explain cbo
select 
    sum(lo_revenue) as lo_revenue, d_year, p_brand1
from 
    lineorder_n4, dates_n4, ssb_part_n4, supplier_n4
where 
    lo_orderdate = d_datekey
    and lo_partkey = p_partkey
    and lo_suppkey = s_suppkey
    and p_brand1 = 'MFGR#2239'
    and s_region = 'EUROPE'
group by 
    d_year, p_brand1
order by 
    d_year, p_brand1;

DROP MATERIALIZED VIEW `ssb_mv_n4`;
