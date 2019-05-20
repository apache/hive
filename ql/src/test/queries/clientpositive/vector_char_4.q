set hive.stats.column.autogather=false;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

drop table if exists vectortab2k;
drop table if exists vectortab2korc;

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

drop table if exists char_lazy_binary_columnar; 
create table char_lazy_binary_columnar(ct char(10), csi char(10), ci char(20), cb char(30), cf char(20), cd char(20), cs char(50)) row format serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe' stored as rcfile;

explain vectorization expression
insert overwrite table char_lazy_binary_columnar select t, si, i, b, f, d, s from vectortab2korc;

-- insert overwrite table char_lazy_binary_columnar select t, si, i, b, f, d, s from vectortab2korc;

-- select count(*) as cnt from char_lazy_binary_columnar group by cs order by cnt asc;
