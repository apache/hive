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



drop table dp_no_mm;
drop table dp_mm;

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

drop table intermediate;


