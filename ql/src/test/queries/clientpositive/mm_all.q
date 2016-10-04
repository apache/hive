set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.exec.dynamic.partition.mode=nonstrict;


-- Force multiple writers when reading
drop table intermediate;
create table intermediate(key int) partitioned by (p int) stored as orc;
insert into table intermediate partition(p='455') select key from src limit 2;
insert into table intermediate partition(p='456') select key from src limit 2;

drop table part_mm;
create table part_mm(key int) partitioned by (key_mm int) stored as orc tblproperties ('hivecommit'='true');
explain insert into table part_mm partition(key_mm='455') select key from intermediate;
insert into table part_mm partition(key_mm='455') select key from intermediate;
insert into table part_mm partition(key_mm='456') select key from intermediate;
insert into table part_mm partition(key_mm='455') select key from intermediate;
select * from part_mm;
drop table part_mm;

drop table simple_mm;
create table simple_mm(key int) stored as orc tblproperties ('hivecommit'='true');
insert into table simple_mm select key from intermediate;
insert overwrite table simple_mm select key from intermediate;
select * from simple_mm;
insert into table simple_mm select key from intermediate;
select * from simple_mm;
drop table simple_mm;


-- simple DP (no bucketing, no sorting?)
drop table dp_no_mm;
drop table dp_mm;

set hive.exec.dynamic.partition.mode=nonstrict;

set hive.merge.mapredfiles=false;
set hive.merge.sparkfiles=false;
set hive.merge.tezfiles=false;

create table dp_no_mm (key int) partitioned by (key1 string, key2 int) stored as orc;
create table dp_mm (key int) partitioned by (key1 string, key2 int) stored as orc
  tblproperties ('hivecommit'='true');

insert into table dp_no_mm partition (key1='123', key2) select key, key from intermediate;

insert into table dp_mm partition (key1='123', key2) select key, key from intermediate;

select * from dp_no_mm;
select * from dp_mm;

drop table dp_no_mm;
drop table dp_mm;


-- union

create table union_mm(id int)  tblproperties ('hivecommit'='true'); 
insert into table union_mm 
select temps.p from (
select key as p from intermediate 
union all 
select key + 1 as p from intermediate ) temps;

select * from union_mm order by id;

insert into table union_mm 
select p from
(
select key + 1 as p from intermediate
union all
select key from intermediate
) tab group by p
union all
select key + 2 as p from intermediate;

select * from union_mm order by id;

insert into table union_mm
SELECT p FROM
(
  SELECT key + 1 as p FROM intermediate
  UNION ALL
  SELECT key as p FROM ( 
    SELECT distinct key FROM (
      SELECT key FROM (
        SELECT key + 2 as key FROM intermediate
        UNION ALL
        SELECT key FROM intermediate
      )t1 
    group by key)t2
  )t3
)t4
group by p;


select * from union_mm order by id;
drop table union_mm;


create table partunion_mm(id int) partitioned by (key int) tblproperties ('hivecommit'='true'); 
insert into table partunion_mm partition(key)
select temps.* from (
select key as p, key from intermediate 
union all 
select key + 1 as p, key + 1 from intermediate ) temps;

select * from partunion_mm;
drop table partunion_mm;

-- TODO# from here, fix it




-- future





--drop table partunion_mm;
--drop table merge_mm;
--drop table ctas_mm;
--drop table T1;
--drop table T2;
--drop table skew_mm;
--
--
--create table ctas_mm tblproperties ('hivecommit'='true') as select * from src limit 3;
--
--create table partunion_mm(id_mm int) partitioned by (key_mm int)  tblproperties ('hivecommit'='true');
--
--
--insert into table partunion_mm partition(key_mm)
--select temps.* from (
--select key as key_mm, key from ctas_mm 
--union all 
--select key as key_mm, key from simple_mm ) temps;
--
--set hive.merge.mapredfiles=true;
--set hive.merge.sparkfiles=true;
--set hive.merge.tezfiles=true;
--
--CREATE TABLE merge_mm (key INT, value STRING) 
--    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC tblproperties ('hivecommit'='true');
--
--EXPLAIN
--INSERT OVERWRITE TABLE merge_mm PARTITION (ds='123', part)
--        SELECT key, value, PMOD(HASH(key), 2) as part
--        FROM src;
--
--INSERT OVERWRITE TABLE merge_mm PARTITION (ds='123', part)
--        SELECT key, value, PMOD(HASH(key), 2) as part
--        FROM src;
--
--
--set hive.optimize.skewjoin.compiletime = true;
---- the test case is wrong?
--
--CREATE TABLE T1(key STRING, val STRING)
--SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;
--LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;
--CREATE TABLE T2(key STRING, val STRING)
--SKEWED BY (key) ON ((3)) STORED AS TEXTFILE;
--LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2;
--
--EXPLAIN
--SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key;
--
--create table skew_mm(k1 string, k2 string, k3 string, k4 string) SKEWED BY (key) ON ((2)) tblproperties ('hivecommit'='true');
--INSERT OVERWRITE TABLE skew_mm
--SELECT a.key as k1, a.val as k2, b.key as k3, b.val as k4 FROM T1 a JOIN T2 b ON a.key = b.key;
--
---- TODO load, multi-insert etc
--
--

drop table intermediate;