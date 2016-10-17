set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.tez.auto.reducer.parallelism=false;

drop table intermediate;
create table intermediate(key int) partitioned by (p int) stored as orc;
insert into table intermediate partition(p='455') select distinct key from src where key >= 0 order by key desc limit 2;
insert into table intermediate partition(p='456') select distinct key from src where key is not null order by key asc limit 2;
insert into table intermediate partition(p='457') select distinct key from src where key >= 100 order by key asc limit 2;


drop table bucket1_mm;
create table bucket1_mm(key int, id int) partitioned by (key2 int)
clustered by (key) sorted by (key) into 2 buckets
tblproperties('hivecommit'='true');
insert into table bucket1_mm partition (key2)
select key + 1, key, key - 1 from intermediate
union all 
select key - 1, key, key + 1 from intermediate;
select * from bucket1_mm;
select * from bucket1_mm tablesample (bucket 1 out of 2) s;
select * from bucket1_mm tablesample (bucket 2 out of 2) s;
drop table bucket1_mm;



drop table intermediate;


