set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.fetch.task.conversion=none;

drop table simple_mm;

  
create table simple_mm(key int) partitioned by (key_mm int) tblproperties ('hivecommit'='true');
insert into table simple_mm partition(key_mm='455') select key from src limit 3;

