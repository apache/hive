--! qt:dataset:part

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;
set hive.stats.column.autogather=true;

CREATE TABLE `customer_ext_n3`(
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

LOAD DATA LOCAL INPATH '../../data/files/ssb/customer/' into table `customer_ext_n3`;

CREATE TABLE `customer_n3`(
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

INSERT INTO `customer_n3`
SELECT * FROM `customer_ext_n3`;

CREATE TABLE `dates_ext_n3`(
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

LOAD DATA LOCAL INPATH '../../data/files/ssb/date/' into table `dates_ext_n3`;

CREATE TABLE `dates_n3`(
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

INSERT INTO `dates_n3`
SELECT * FROM `dates_ext_n3`;

CREATE TABLE `ssb_part_ext_n3`(
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

LOAD DATA LOCAL INPATH '../../data/files/ssb/part/' into table `ssb_part_ext_n3`;

CREATE TABLE `ssb_part_n3`(
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

INSERT INTO `ssb_part_n3`
SELECT * FROM `ssb_part_ext_n3`;

CREATE TABLE `supplier_ext_n3`(
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

LOAD DATA LOCAL INPATH '../../data/files/ssb/supplier/' into table `supplier_ext_n3`;

CREATE TABLE `supplier_n3`(
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

INSERT INTO `supplier_n3`
SELECT * FROM `supplier_ext_n3`;

CREATE TABLE `lineorder_ext_n3`(
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

LOAD DATA LOCAL INPATH '../../data/files/ssb/lineorder/' into table `lineorder_ext_n3`;

CREATE TABLE `lineorder_n3`(
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
  constraint fk1 foreign key (`lo_custkey`) references `customer_n3`(`c_custkey`) disable rely,
  constraint fk2 foreign key (`lo_orderdate`) references `dates_n3`(`d_datekey`) disable rely,
  constraint fk3 foreign key (`lo_partkey`) references `ssb_part_n3`(`p_partkey`) disable rely,
  constraint fk4 foreign key (`lo_suppkey`) references `supplier_n3`(`s_suppkey`) disable rely)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

INSERT INTO `lineorder_n3`
SELECT * FROM `lineorder_ext_n3`;

CREATE MATERIALIZED VIEW `ssb_mv_n3`
PARTITIONED ON (GROUPING__ID)
AS
SELECT
  c_city,
  c_nation,
  c_region,
  d_weeknuminyear,
  d_year,
  d_yearmonth,
  d_yearmonthnum,
  lo_discount,
  lo_quantity,
  p_brand1,
  p_category,
  p_mfgr,
  s_city,
  s_nation,
  s_region,
  GROUPING__ID,
  sum(lo_revenue),
  sum(lo_extendedprice * lo_discount) discounted_price,
  sum(lo_revenue - lo_supplycost) net_revenue
FROM
  customer_n3, dates_n3, lineorder_n3, ssb_part_n3, supplier_n3
WHERE
  lo_orderdate = d_datekey
  and lo_partkey = p_partkey
  and lo_suppkey = s_suppkey
  and lo_custkey = c_custkey
GROUP BY
  c_city,
  c_nation,
  c_region,
  d_weeknuminyear,
  d_year,
  d_yearmonth,
  d_yearmonthnum,
  lo_discount,
  lo_quantity,
  p_brand1,
  p_category,
  p_mfgr,
  s_city,
  s_nation,
  s_region
GROUPING SETS (
  (d_year, d_yearmonthnum, d_weeknuminyear, lo_discount, lo_quantity),
  (d_year, d_yearmonth, c_city, c_nation, c_region, s_city, s_nation, s_region, p_brand1, p_category, p_mfgr)
);

-- Q1.1
explain cbo
select 
    sum(lo_extendedprice*lo_discount) as revenue
from 
    lineorder_n3, dates_n3
where 
    lo_orderdate = d_datekey
    and d_year = 1993
    and lo_discount between 1 and 3
    and lo_quantity < 25;

-- Q1.2
explain cbo
select 
    sum(lo_extendedprice*lo_discount) as revenue
from 
    lineorder_n3, dates_n3
where 
    lo_orderdate = d_datekey
    and d_yearmonthnum = 199401
    and lo_discount between 4 and 6
    and lo_quantity between 26 and 35;

-- Q1.3
explain cbo
select 
    sum(lo_extendedprice*lo_discount) as revenue
from 
    lineorder_n3, dates_n3
where 
    lo_orderdate = d_datekey
    and d_weeknuminyear = 6
    and d_year = 1994
    and lo_discount between 5 and 7
    and lo_quantity between 26 and 35;

-- Q2.1
explain cbo
select 
    sum(lo_revenue) as lo_revenue, d_year, p_brand1
from 
    lineorder_n3, dates_n3, ssb_part_n3, supplier_n3
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
    lineorder_n3, dates_n3, ssb_part_n3, supplier_n3
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
    lineorder_n3, dates_n3, ssb_part_n3, supplier_n3
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

-- Q3.1
explain cbo
select 
    c_nation, s_nation, d_year,
    sum(lo_revenue) as lo_revenue
from 
    customer_n3, lineorder_n3, supplier_n3, dates_n3
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_orderdate = d_datekey
    and c_region = 'ASIA'
    and s_region = 'ASIA'
    and d_year >= 1992 and d_year <= 1997
group by 
    c_nation, s_nation, d_year
order by 
    d_year asc, lo_revenue desc;

-- Q3.2
explain cbo
select 
    c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
from 
    customer_n3, lineorder_n3, supplier_n3, dates_n3
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_orderdate = d_datekey
    and c_nation = 'UNITED STATES'
    and s_nation = 'UNITED STATES'
    and d_year >= 1992 and d_year <= 1997
group by 
    c_city, s_city, d_year
order by 
    d_year asc, lo_revenue desc;

-- Q3.3
explain cbo
select 
    c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
from 
    customer_n3, lineorder_n3, supplier_n3, dates_n3
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_orderdate = d_datekey
    and (c_city='UNITED KI1' or c_city='UNITED KI5')
    and (s_city='UNITED KI1' or s_city='UNITED KI5')
    and d_year >= 1992 and d_year <= 1997
group by 
    c_city, s_city, d_year
order by 
    d_year asc, lo_revenue desc;

-- Q3.4
explain cbo
select 
    c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
from 
    customer_n3, lineorder_n3, supplier_n3, dates_n3
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_orderdate = d_datekey
    and (c_city='UNITED KI1' or c_city='UNITED KI5')
    and (s_city='UNITED KI1' or s_city='UNITED KI5')
    and d_yearmonth = 'Dec1997'
group by 
    c_city, s_city, d_year
order by 
    d_year asc, lo_revenue desc;

-- Q4.1
explain cbo
select 
    d_year, c_nation,
    sum(lo_revenue - lo_supplycost) as profit
from 
    dates_n3, customer_n3, supplier_n3, ssb_part_n3, lineorder_n3
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_partkey = p_partkey
    and lo_orderdate = d_datekey
    and c_region = 'AMERICA'
    and s_region = 'AMERICA'
    and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by 
    d_year, c_nation
order by 
    d_year, c_nation;

-- Q4.2
explain cbo
select 
    d_year, s_nation, p_category,
    sum(lo_revenue - lo_supplycost) as profit
from 
    dates_n3, customer_n3, supplier_n3, ssb_part_n3, lineorder_n3
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_partkey = p_partkey
    and lo_orderdate = d_datekey
    and c_region = 'AMERICA'
    and s_region = 'AMERICA'
    and (d_year = 1997 or d_year = 1998)
    and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by 
    d_year, s_nation, p_category
order by 
    d_year, s_nation, p_category;

-- Q4.3
explain cbo
select 
    d_year, s_city, p_brand1,
    sum(lo_revenue - lo_supplycost) as profit
from 
    dates_n3, customer_n3, supplier_n3, ssb_part_n3, lineorder_n3
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_partkey = p_partkey
    and lo_orderdate = d_datekey
    and c_region = 'AMERICA'
    and s_nation = 'UNITED STATES'
    and (d_year = 1997 or d_year = 1998)
    and p_category = 'MFGR#14'
group by 
    d_year, s_city, p_brand1
order by 
    d_year, s_city, p_brand1;

DROP MATERIALIZED VIEW `ssb_mv_n3`;
