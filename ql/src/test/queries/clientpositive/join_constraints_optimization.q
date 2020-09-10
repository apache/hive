set hive.strict.checks.cartesian.product=false;

CREATE TABLE `customer_removal_n0`(
  `c_custkey` bigint, 
  `c_name` string, 
  `c_address` string, 
  `c_city` string, 
  `c_nation` string, 
  `c_region` string, 
  `c_phone` string, 
  `c_mktsegment` string,
  primary key (`c_custkey`) disable rely)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE `dates_removal_n0`(
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
  primary key (`d_datekey`) disable rely)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE `lineorder_removal_n0`(
  `lo_orderkey` bigint, 
  `lo_linenumber` int, 
  `lo_custkey` bigint not null disable rely,
  `lo_partkey` bigint not null disable rely,
  `lo_suppkey` bigint not null disable rely,
  `lo_orderdate` bigint,
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
  constraint fk1 foreign key (`lo_custkey`) references `customer_removal_n0`(`c_custkey`) disable rely,
  constraint fk2 foreign key (`lo_orderdate`) references `dates_removal_n0`(`d_datekey`) disable rely)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

-- CAN BE REMOVED AND DOES NOT NEED FILTER ON JOIN COLUMN
-- AS COLUMN IS ALREADY NOT NULLABLE
EXPLAIN
SELECT `lo_linenumber`
FROM `lineorder_removal_n0`
JOIN `customer_removal_n0` ON `lo_custkey` = `c_custkey`;

-- CAN BE REMOVED AND INTRODUCES A FILTER ON JOIN COLUMN
EXPLAIN
SELECT `lo_linenumber`
FROM `lineorder_removal_n0`
JOIN `dates_removal_n0` ON `lo_orderdate` = `d_datekey`;

-- REMOVES THE JOIN
EXPLAIN
SELECT `lo_linenumber`
FROM `lineorder_removal_n0`
LEFT OUTER JOIN `customer_removal_n0` ON `lo_custkey` = `c_custkey`;

-- TRANSFORMS THE JOIN
EXPLAIN
SELECT `lo_linenumber`, `c_region`
FROM `lineorder_removal_n0`
LEFT OUTER JOIN `customer_removal_n0` ON `lo_custkey` = `c_custkey`;

-- NOT TRANSFORMED INTO INNER JOIN SINCE JOIN COLUMN IS NULLABLE
EXPLAIN
SELECT `lo_linenumber`
FROM `lineorder_removal_n0`
LEFT OUTER JOIN `dates_removal_n0` ON `lo_orderdate` = `d_datekey`;

-- REMOVES BOTH JOINS
EXPLAIN
SELECT `lo_linenumber`
FROM `lineorder_removal_n0`
JOIN `customer_removal_n0` ON `lo_custkey` = `c_custkey`
JOIN `dates_removal_n0` ON `lo_orderdate` = `d_datekey`;

-- REMOVES BOTH JOINS
EXPLAIN
SELECT `lo_linenumber`
FROM `lineorder_removal_n0`
LEFT OUTER JOIN `customer_removal_n0` ON `lo_custkey` = `c_custkey`
JOIN `dates_removal_n0` ON `lo_orderdate` = `d_datekey`;

-- REMOVE INNER AND NOT TRANFORM OUTER
EXPLAIN
SELECT `lo_linenumber` FROM
(SELECT *
FROM `lineorder_removal_n0`
JOIN `customer_removal_n0` ON `lo_custkey` = `c_custkey`) subq
LEFT OUTER JOIN `dates_removal_n0` ON `lo_orderdate` = `d_datekey`;

-- REMOVE FIRST OUTER AND NOT TRANFORM SECOND OUTER
EXPLAIN
SELECT `lo_linenumber`
FROM `lineorder_removal_n0`
LEFT OUTER JOIN `customer_removal_n0` ON `lo_custkey` = `c_custkey`
LEFT OUTER JOIN `dates_removal_n0` ON `lo_orderdate` = `d_datekey`;

-- REMOVE SECOND OUTER AND NOT TRANFORM FIRST OUTER
EXPLAIN
SELECT `lo_linenumber`
FROM `lineorder_removal_n0`
LEFT OUTER JOIN `dates_removal_n0` ON `lo_orderdate` = `d_datekey`
LEFT OUTER JOIN `customer_removal_n0` ON `lo_custkey` = `c_custkey`;

-- SWAP AND REMOVE
EXPLAIN
SELECT `lo_linenumber`, `c_custkey`
FROM `lineorder_removal_n0`
JOIN `customer_removal_n0` ON `lo_custkey` = `c_custkey`;

-- FK-PK JOIN with FK side removal
EXPLAIN
SELECT customer_removal_n0.*
FROM customer_removal_n0
    JOIN
    (SELECT lo_custkey
    FROM lineorder_removal_n0
    WHERE lo_custkey IS NOT NULL
    GROUP BY lo_custkey) fkSide ON fkSide.lo_custkey = customer_removal_n0.c_custkey;

-- FK-PK JOIN with FK side removal
EXPLAIN
SELECT customer_removal_n0.*
FROM customer_removal_n0
         JOIN
     (SELECT lo_custkey, sum(lo_discount)  sm
      FROM lineorder_removal_n0
      WHERE lo_custkey IS NOT NULL
      GROUP BY lo_custkey) fkSide ON fkSide.lo_custkey = customer_removal_n0.c_custkey
WHERE fkSide.sm > 0;

-- FK-PK JOIN with FK side removal, BUT without explicit IS NOT NULL on join column
EXPLAIN
SELECT customer_removal_n0.*
FROM customer_removal_n0
    JOIN (SELECT lo_custkey
            FROM lineorder_removal_n0
            GROUP BY lo_custkey) fkSide on fkSide.lo_custkey = customer_removal_n0.c_custkey;

-- NEGATIVE for FK-PK JOIN with FK side removal, FK JOIN COL might not be distinct
EXPLAIN
SELECT customer_removal_n0.*
FROM customer_removal_n0
         JOIN (SELECT lo_linenumber,lo_custkey
               FROM lineorder_removal_n0
               WHERE lo_custkey IS NOT NULL
               GROUP BY lo_linenumber, lo_custkey,lo_custkey
             ) fkSide ON fkSide.lo_custkey = customer_removal_n0.c_custkey;

-- FK side join col is PK as well (thus providing uniqueness without GROUP BY)
CREATE TABLE `t1`(
               `lo_orderkey` bigint,
               `lo_linenumber` int,
               `lo_custkey` bigint not null disable rely,
               `lo_partkey` bigint not null disable rely,
               `lo_orderdate` bigint,
               `lo_revenue` double,
               primary key (`lo_custkey`) disable rely,
               constraint fkt1 foreign key (`lo_custkey`) references `customer_removal_n0`(`c_custkey`) disable rely,
               constraint fkt2 foreign key (`lo_orderdate`) references `dates_removal_n0`(`d_datekey`) disable rely)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE;

EXPLAIN
SELECT customer_removal_n0.*
FROM customer_removal_n0
         JOIN
     (SELECT lo_custkey,lo_linenumber
      FROM t1
      WHERE lo_custkey IS NOT NULL) fkSide ON fkSide.lo_custkey = customer_removal_n0.c_custkey;
DROP TABLE t1;

-- FK side join col is UNIQUE and NOT NULL(thus providing uniqueness without GROUP BY)
CREATE TABLE `t1`(
                     `lo_orderkey` bigint,
                     `lo_linenumber` int,
                     `lo_custkey` bigint not null disable rely,
                     `lo_partkey` bigint not null disable rely,
                     `lo_orderdate` bigint,
                     `lo_revenue` double,
                     UNIQUE (`lo_custkey`) disable rely,
                     constraint fkt1 foreign key (`lo_custkey`) references `customer_removal_n0`(`c_custkey`) disable rely,
                     constraint fkt2 foreign key (`lo_orderdate`) references `dates_removal_n0`(`d_datekey`) disable rely)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE;


EXPLAIN
SELECT customer_removal_n0.*
FROM customer_removal_n0
         JOIN
     (SELECT lo_custkey,lo_linenumber
      FROM t1) fkSide ON fkSide.lo_custkey = customer_removal_n0.c_custkey;

DROP TABLE t1;