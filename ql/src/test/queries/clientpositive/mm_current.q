set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set  hive.tez.auto.reducer.parallelism=false;

create table intermediate(key int) partitioned by (p int) stored as orc;
insert into table intermediate partition(p='455') select key from src limit 3;
insert into table intermediate partition(p='456') select key from src limit 3;
insert into table intermediate partition(p='457') select key from src limit 3;
  
create table simple_mm(key int) partitioned by (key_mm int) stored as orc tblproperties ('hivecommit'='true');

explain insert into table simple_mm partition(key_mm='455') select key from intermediate;
insert into table simple_mm partition(key_mm='455') select key from intermediate;

drop table simple_mm;
drop table intermediate;

