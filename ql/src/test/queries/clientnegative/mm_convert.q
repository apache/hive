set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.exec.dynamic.partition.mode=nonstrict;

drop table convert_mm;
create table convert_mm(key int, id int) tblproperties("transactional"="true", "transactional_properties"="insert_only");
alter table convert_mm unset tblproperties('transactional_properties', 'transactional');


