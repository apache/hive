SET hive.exec.schema.evolution=false;
create table src_orc (key tinyint, val string) clustered by (key) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
alter table src_orc change key k tinyint first;
