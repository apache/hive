set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.exec.dynamic.partition.mode=nonstrict;

drop table bucket0_mm;
drop table bucket1_mm;
create table bucket0_mm(key int, id int) clustered by (key) into 2 buckets
  tblproperties("transactional"="true", "transactional_properties"="insert_only");
create table bucket1_mm(key int, id int) clustered by (key) into 2 buckets
  tblproperties("transactional"="true", "transactional_properties"="insert_only");

set hive.strict.checks.bucketing=false;
alter table bucket0_mm unset tblproperties('transactional_properties', 'transactional');
set hive.strict.checks.bucketing=true;
alter table bucket1_mm unset tblproperties('transactional_properties', 'transactional');


