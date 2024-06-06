set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;
set hive.auto.convert.join=true;

CREATE TABLE T1_n39(key STRING, value STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n39;

drop table array_valued_T1_n39;
create table array_valued_T1_n39 (key string, value array<string>) SKEWED BY (key) ON ((8));
insert overwrite table array_valued_T1_n39 select key, array(value) from T1_n39;

-- copy from skewjoinopt10
-- test compile time skew join and auto map join
-- This test is to verify the skew join compile optimization when the join is followed by a lateral view
-- adding a order by at the end to make the results deterministic

explain 
select * from (select a.key as key, b.value as array_val from T1_n39 a join array_valued_T1_n39 b on a.key=b.key) i lateral view explode (array_val) c as val;

select * from (select a.key as key, b.value as array_val from T1_n39 a join array_valued_T1_n39 b on a.key=b.key) i lateral view explode (array_val) c as val
ORDER BY key, val;
