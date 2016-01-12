SET hive.exec.schema.evolution=false;
create table src_orc (key smallint, val string) stored as orc TBLPROPERTIES ('transactional'='true');
alter table src_orc replace columns (k int, val string, z smallint);
alter table src_orc replace columns (k int, val string, z tinyint);
