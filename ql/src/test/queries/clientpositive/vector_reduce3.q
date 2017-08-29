set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reducesink.new.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

create table vectortab2k(
            t tinyint,
            si smallint,
            i int,
            b bigint,
            f float,
            d double,
            dc decimal(38,18),
            bo boolean,
            s string,
            s2 string,
            ts timestamp,
            ts2 timestamp,
            dt date)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/vectortab2k' OVERWRITE INTO TABLE vectortab2k;

create table vectortab2korc(
            t tinyint,
            si smallint,
            i int,
            b bigint,
            f float,
            d double,
            dc decimal(38,18),
            bo boolean,
            s string,
            s2 string,
            ts timestamp,
            ts2 timestamp,
            dt date)
STORED AS ORC;

INSERT INTO TABLE vectortab2korc SELECT * FROM vectortab2k;

explain vectorization expression
select s from vectortab2korc order by s;

select s from vectortab2korc order by s;
