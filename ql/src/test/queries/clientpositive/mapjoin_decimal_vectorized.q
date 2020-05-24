SET hive.vectorized.execution.enabled=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000000;

-- SORT_QUERY_RESULTS

CREATE TABLE over1k_n5(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE over1k_n5;

CREATE TABLE t1_n95(`dec` decimal(4,2)) STORED AS ORC;
INSERT INTO TABLE t1_n95 select `dec` from over1k_n5;
CREATE TABLE t2_n59(`dec` decimal(4,0)) STORED AS ORC;
INSERT INTO TABLE t2_n59 select `dec` from over1k_n5;


set hive.mapjoin.optimized.hashtable=false;

explain vectorization detail select t1_n95.`dec`, t2_n59.`dec` from t1_n95 join t2_n59 on (t1_n95.`dec`=t2_n59.`dec`) order by t1_n95.`dec`;

select t1_n95.`dec`, t2_n59.`dec` from t1_n95 join t2_n59 on (t1_n95.`dec`=t2_n59.`dec`) order by t1_n95.`dec`;

set hive.mapjoin.optimized.hashtable=true;

select t1_n95.`dec`, t2_n59.`dec` from t1_n95 join t2_n59 on (t1_n95.`dec`=t2_n59.`dec`) order by t1_n95.`dec`;

explain vectorization detail select t1_n95.`dec`, t2_n59.`dec` from t1_n95 join t2_n59 on (t1_n95.`dec`=t2_n59.`dec`) order by t1_n95.`dec`;

set hive.cbo.enable=false;

set hive.mapjoin.optimized.hashtable=false;

explain vectorization detail select t1_n95.`dec`, t2_n59.`dec` from t1_n95 join t2_n59 on (t1_n95.`dec`=t2_n59.`dec`) order by t1_n95.`dec`;

select t1_n95.`dec`, t2_n59.`dec` from t1_n95 join t2_n59 on (t1_n95.`dec`=t2_n59.`dec`) order by t1_n95.`dec`;

set hive.mapjoin.optimized.hashtable=true;

select t1_n95.`dec`, t2_n59.`dec` from t1_n95 join t2_n59 on (t1_n95.`dec`=t2_n59.`dec`) order by t1_n95.`dec`;

explain vectorization detail select t1_n95.`dec`, t2_n59.`dec` from t1_n95 join t2_n59 on (t1_n95.`dec`=t2_n59.`dec`) order by t1_n95.`dec`;