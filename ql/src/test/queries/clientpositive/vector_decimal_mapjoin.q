-- until HIVE-18225 is fixed
set hive.stats.column.autogather=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

CREATE TABLE over1k_n2(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(20,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE over1k_n2;

CREATE TABLE t1_n48(`dec` decimal(22,2), value_dec decimal(22,2)) STORED AS ORC;
INSERT INTO TABLE t1_n48 select `dec`, cast(d as decimal(22,2)) from over1k_n2;
CREATE TABLE t2_n29(`dec` decimal(24,0), value_dec decimal(24,0)) STORED AS ORC;
INSERT INTO TABLE t2_n29 select `dec`, cast(d as decimal(24,0)) from over1k_n2;

explain vectorization detail
select t1_n48.`dec`, t2_n29.`dec` from t1_n48 join t2_n29 on (t1_n48.`dec`=t2_n29.`dec`);

-- SORT_QUERY_RESULTS

select t1_n48.`dec`, t2_n29.`dec` from t1_n48 join t2_n29 on (t1_n48.`dec`=t2_n29.`dec`);
select count(*) from (select t1_n48.`dec`, t2_n29.`dec` from t1_n48 join t2_n29 on (t1_n48.`dec`=t2_n29.`dec`)) as t;

explain vectorization detail
select t1_n48.`dec`, t1_n48.value_dec, t2_n29.`dec`, t2_n29.value_dec from t1_n48 join t2_n29 on (t1_n48.`dec`=t2_n29.`dec`);

select t1_n48.`dec`, t1_n48.value_dec, t2_n29.`dec`, t2_n29.value_dec from t1_n48 join t2_n29 on (t1_n48.`dec`=t2_n29.`dec`);
select count(*) from (select t1_n48.`dec`, t1_n48.value_dec, t2_n29.`dec`, t2_n29.value_dec from t1_n48 join t2_n29 on (t1_n48.`dec`=t2_n29.`dec`)) as t;



-- DECIMAL_64

CREATE TABLE over1k_small(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(14,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE over1k_small;

CREATE TABLE t1_small(`dec` decimal(14,2), value_dec decimal(14,2)) STORED AS TEXTFILE;
INSERT INTO TABLE t1_small select `dec`, cast(d as decimal(14,2)) from over1k_small;
CREATE TABLE t2_small(`dec` decimal(14,0), value_dec decimal(14,0)) STORED AS TEXTFILE;
INSERT INTO TABLE t2_small select `dec`, cast(d as decimal(14,0)) from over1k_small;

explain vectorization detail
select t1_small.`dec`, t2_small.`dec` from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`);

-- SORT_QUERY_RESULTS

select t1_small.`dec`, t2_small.`dec` from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`);
select count(*) from (select t1_small.`dec`, t2_small.`dec` from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`)) as t;

explain vectorization detail
select t1_small.`dec`, t1_small.value_dec, t2_small.`dec`, t2_small.value_dec from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`);

select t1_small.`dec`, t1_small.value_dec, t2_small.`dec`, t2_small.value_dec from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`);
select count(*) from (select t1_small.`dec`, t1_small.value_dec, t2_small.`dec`, t2_small.value_dec from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`)) as t;


set hive.vectorized.input.format.supports.enabled=none;

explain vectorization detail
select t1_small.`dec`, t2_small.`dec` from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`);

-- SORT_QUERY_RESULTS

select t1_small.`dec`, t2_small.`dec` from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`);
select count(*) from (select t1_small.`dec`, t2_small.`dec` from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`)) as t;

explain vectorization detail
select t1_small.`dec`, t1_small.value_dec, t2_small.`dec`, t2_small.value_dec from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`);

select t1_small.`dec`, t1_small.value_dec, t2_small.`dec`, t2_small.value_dec from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`);
select count(*) from (select t1_small.`dec`, t1_small.value_dec, t2_small.`dec`, t2_small.value_dec from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`)) as t;

