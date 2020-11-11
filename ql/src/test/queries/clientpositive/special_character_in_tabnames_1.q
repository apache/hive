--! qt:dataset:src1
--! qt:dataset:src
set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;
set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;
set hive.strict.checks.cartesian.product=false;

-- SORT_QUERY_RESULTS

create database `db~!@#$%^&*(),<>`;
use `db~!@#$%^&*(),<>`;

create table `c/b/o_t1`(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table `//cbo_t2`(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table `cbo_/t3////`(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;

load data local inpath '../../data/files/cbo_t1.txt' into table `c/b/o_t1` partition (dt='2014');
load data local inpath '../../data/files/cbo_t2.txt' into table `//cbo_t2` partition (dt='2014');
load data local inpath '../../data/files/cbo_t3.txt' into table `cbo_/t3////`;

CREATE TABLE `p/a/r/t`(
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
);

LOAD DATA LOCAL INPATH '../../data/files/tpch/tiny/part.tbl.bz2' overwrite into table `p/a/r/t`;

CREATE TABLE `line/item` (L_ORDERKEY      INT,
                                L_PARTKEY       INT,
                                L_SUPPKEY       INT,
                                L_LINENUMBER    INT,
                                L_QUANTITY      DOUBLE,
                                L_EXTENDEDPRICE DOUBLE,
                                L_DISCOUNT      DOUBLE,
                                L_TAX           DOUBLE,
                                L_RETURNFLAG    STRING,
                                L_LINESTATUS    STRING,
                                l_shipdate      STRING,
                                L_COMMITDATE    STRING,
                                L_RECEIPTDATE   STRING,
                                L_SHIPINSTRUCT  STRING,
                                L_SHIPMODE      STRING,
                                L_COMMENT       STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '../../data/files/tpch/tiny/lineitem.tbl.bz2' OVERWRITE INTO TABLE `line/item`;

create table `src/_/cbo` as select * from default.src;

analyze table `c/b/o_t1` compute statistics for columns key, value, c_int, c_float, c_boolean;

analyze table `//cbo_t2` compute statistics for columns key, value, c_int, c_float, c_boolean;

analyze table `cbo_/t3////` compute statistics for columns key, value, c_int, c_float, c_boolean;

analyze table `src/_/cbo` compute statistics for columns;

analyze table `p/a/r/t` compute statistics for columns;

analyze table `line/item` compute statistics for columns;


set hive.cbo.enable=false;
set hive.exec.check.crossproducts=false;
set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

































-- agg, corr

explain select p_mfgr, p_name, p_size

from `p/a/r/t` b where b.p_size not in

  (select min(p_size)

  from (select p_mfgr, p_size from `p/a/r/t`) a

  where p_size < 10 and b.p_mfgr = a.p_mfgr

  
) order by  p_name

;


