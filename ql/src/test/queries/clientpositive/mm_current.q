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


set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.orcfile.stripe.level=true;


drop table merge2_mm;
create table merge2_mm(key int) tblproperties('hivecommit'='true');
insert overwrite table merge2_mm select key from intermediate;
select * from merge2_mm;
drop table merge2_mm;




drop table intermediate;


