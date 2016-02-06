SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
set hive.explain.user=false;

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

create temporary table temptable1 (
                     t tinyint,
                     si smallint,
                     i int,
                     b bigint,
                     f float,
                     d double,
                     dc decimal(38,18),
                     bo boolean,
                     s string,
                     vc varchar(50),
                     ch char(50),
                     ts timestamp,
                     dt date,
                     ar array<int>,
                     st struct<col1:string,col2:int>,
                     ma map<string, int>
                   ) stored as orc;

explain
insert overwrite table temptable1
                     select t, si, i, b, f, d, dc, bo, s, s, s, ts, dt,
                     array(i, i+1), struct(s, i), map(s, i) from vectortab2korc where s='mathematics';
explain
select count(*) from temptable1;

insert overwrite table temptable1
                     select t, si, i, b, f, d, dc, bo, s, s, s, ts, dt,
                     array(i, i+1), struct(s, i), map(s, i) from vectortab2korc where s='mathematics';
select count(*) from temptable1;