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


set hive.optimize.skewjoin.compiletime = true;

create table skew_mm(k1 int, k2 int, k4 int) skewed by (k1, k4) on ((0,0),(1,1),(2,2),(3,3))
 stored as directories tblproperties ('hivecommit'='false');

insert into table skew_mm 
select key, key, key from intermediate;

drop table skew_mm;


create table skew_mm(k1 int, k2 int, k4 int) skewed by (k1, k4) on ((0,0),(1,1),(2,2),(3,3))
 stored as directories tblproperties ('hivecommit'='true');

insert into table skew_mm 
select key, key, key from intermediate;

select * from skew_mm;
drop table skew_mm;





create table skew_mm(k1 int, k2 int, k4 int) partitioned by (k3 int) 
skewed by (k1, k4) on ((0,0),(1,1),(2,2),(3,3)) stored as directories tblproperties ('hivecommit'='true');

insert into table skew_mm partition (k3)
select key as i, key as j, key as k, key as l from intermediate
union all 
select key +1 as i, key +2 as j, key +3 as k, key +4 as l from intermediate;


select * from skew_mm;
drop table skew_mm;



drop table intermediate;


