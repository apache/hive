set hive.update.partitions.properties.enabled=true;
create table alter1(a int, b int) partitioned by (insertdate string);
alter table alter1 add partition (insertdate='2008-01-01') location '2008/01/01';
desc formatted alter1 partition(insertdate='2008-01-01');
alter table alter1 partition(insertdate='2008-01-01') set tblproperties ('a'='1', 'c'='3');
desc formatted alter1 partition(insertdate='2008-01-01');
alter table alter1 partition(insertdate='2008-01-01') unset tblproperties if exists ('c'='3');
desc formatted alter1 partition(insertdate='2008-01-01');