SET hive.vectorized.execution.enabled=true;

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

drop table if exists varchar_lazy_binary_columnar; 
create table varchar_lazy_binary_columnar(vt varchar(10), vsi varchar(10), vi varchar(20), vb varchar(30), vf varchar(20),vd varchar(20),vs varchar(50)) row format serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe' stored as rcfile;

explain
insert overwrite table varchar_lazy_binary_columnar select t, si, i, b, f, d, s from vectortab2korc;

-- insert overwrite table varchar_lazy_binary_columnar select t, si, i, b, f, d, s from vectortab2korc;

-- select count(*) as cnt from varchar_lazy_binary_columnar group by vs order by cnt asc;