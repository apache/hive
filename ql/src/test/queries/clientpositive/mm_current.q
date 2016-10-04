set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.tez.auto.reducer.parallelism=false;

drop table intermediate;
create table intermediate(key int) partitioned by (p int) stored as orc;
insert into table intermediate partition(p='455') select key from src limit 2;
insert into table intermediate partition(p='456') select key from src limit 2;


create table partunion_no_mm(id int) partitioned by (key int); 
insert into table partunion_no_mm partition(key)
select temps.* from (
select key as p, key from intermediate 
union all 
select key + 1 as p, key + 1 from intermediate ) temps;

select * from partunion_no_mm;
drop table partunion_no_mm;


create table partunion_mm(id int) partitioned by (key int) tblproperties ('hivecommit'='true'); 
insert into table partunion_mm partition(key)
select temps.* from (
select key as p, key from intermediate 
union all 
select key + 1 as p, key + 1 from intermediate ) temps;

select * from partunion_mm;
drop table partunion_mm;



drop table intermediate;


