drop table ss_src1;
drop table ss_src2;
drop table ss_src3;
drop table ss_i_part;
drop table ss_t3;
drop table ss_t4;
drop table ss_t5;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=300;
set mapred.min.split.size=300;
set mapred.min.split.size.per.node=300;
set mapred.min.split.size.per.rack=300;
set hive.merge.smallfiles.avgsize=1;

-- create multiple file inputs (two enable multiple splits)
create table ss_i_part (key int, value string) partitioned by (p string);
insert overwrite table ss_i_part partition (p='1') select key, value from src;
insert overwrite table ss_i_part partition (p='2') select key, value from src;
insert overwrite table ss_i_part partition (p='3') select key, value from src;
create table ss_src2 as select key, value from ss_i_part;
select count(1) from ss_src2 tablesample(1 percent);

-- sample first split
desc ss_src2;
set hive.sample.seednumber=0;
explain select key, value from ss_src2 tablesample(1 percent) limit 10;
select key, value from ss_src2 tablesample(1 percent) limit 10;

-- verify seed number of sampling
insert overwrite table ss_i_part partition (p='1') select key+10000, value from src;
insert overwrite table ss_i_part partition (p='2') select key+20000, value from src;
insert overwrite table ss_i_part partition (p='3') select key+30000, value from src;
create table ss_src3 as select key, value from ss_i_part;
set hive.sample.seednumber=3;
create table ss_t3 as select sum(key) % 397 as s from ss_src3 tablesample(1 percent) limit 10;
set hive.sample.seednumber=4;
create table ss_t4 as select sum(key) % 397 as s from ss_src3 tablesample(1 percent) limit 10;
set hive.sample.seednumber=5;
create table ss_t5 as select sum(key) % 397 as s from ss_src3 tablesample(1 percent) limit 10;
select sum(s) from (select s from ss_t3 union all select s from ss_t4 union all select s from ss_t5) t;

-- sample more than one split
explain select count(distinct key) from ss_src2 tablesample(70 percent) limit 10;
select count(distinct key) from ss_src2 tablesample(70 percent) limit 10;

-- sample all splits
select count(1) from ss_src2 tablesample(100 percent);

-- subquery
explain select key from (select key from ss_src2 tablesample(1 percent) limit 10) subq;
select key from (select key from ss_src2 tablesample(1 percent) limit 10) subq;

-- groupby
select key, count(1) from ss_src2 tablesample(1 percent) group by key order by key;

-- sample one of two tables:
create table ss_src1 as select * from ss_src2;
select t2.key as k from ss_src1 join ss_src2 tablesample(1 percent) t2 on ss_src1.key=t2.key order by k;

-- sample two tables
explain select * from (
select t1.key as k1, t2.key as k from ss_src1 tablesample(80 percent) t1 full outer join ss_src2 tablesample(2 percent) t2 on t1.key=t2.key
) subq where k in (199, 10199, 20199) or k1 in (199, 10199, 20199);

select * from (
select t1.key as k1, t2.key as k from ss_src1 tablesample(80 percent) t1 full outer join ss_src2 tablesample(2 percent) t2 on t1.key=t2.key
) subq where k in (199, 10199, 20199) or k1 in (199, 10199, 20199);

-- shrink last split
explain select count(1) from ss_src2 tablesample(1 percent);
set mapred.max.split.size=300000;
set mapred.min.split.size=300000;
set mapred.min.split.size.per.node=300000;
set mapred.min.split.size.per.rack=300000;
select count(1) from ss_src2 tablesample(1 percent);
select count(1) from ss_src2 tablesample(50 percent);


drop table ss_src1;
drop table ss_src2;
drop table ss_src3;
drop table ss_i_part;
drop table ss_t3;
drop table ss_t4;
drop table ss_t5;
