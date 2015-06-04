SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;

create table vectortab_a_1k(
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

LOAD DATA LOCAL INPATH '../../data/files/vectortab_a_1k' OVERWRITE INTO TABLE vectortab_a_1k;

CREATE TABLE vectortab_a_1korc STORED AS ORC AS SELECT * FROM vectortab_a_1k;

create table vectortab_b_1k(
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

LOAD DATA LOCAL INPATH '../../data/files/vectortab_b_1k' OVERWRITE INTO TABLE vectortab_b_1k;

CREATE TABLE vectortab_b_1korc STORED AS ORC AS SELECT * FROM vectortab_b_1k;

explain
select
   v1.s,
   v2.s,
   v1.intrvl1 
from
   ( select
      s,
      (cast(dt as date) - cast(ts as date)) as intrvl1 
   from
      vectortab_a_1korc ) v1 
join
   (
      select
         s ,
         (cast(dt as date) - cast(ts as date)) as intrvl2 
      from
         vectortab_b_1korc 
   ) v2 
      on v1.intrvl1 = v2.intrvl2 
      and v1.s = v2.s;

select
   v1.s,
   v2.s,
   v1.intrvl1 
from
   ( select
      s,
      (cast(dt as date) - cast(ts as date)) as intrvl1 
   from
      vectortab_a_1korc ) v1 
join
   (
      select
         s ,
         (cast(dt as date) - cast(ts as date)) as intrvl2 
      from
         vectortab_b_1korc 
   ) v2 
      on v1.intrvl1 = v2.intrvl2 
      and v1.s = v2.s;