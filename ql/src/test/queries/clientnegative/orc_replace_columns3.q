SET hive.exec.schema.evolution=true;
create table src_orc (key smallint, val string) stored as orc;
alter table src_orc replace columns (k int, val string, z smallint);
alter table src_orc replace columns (k int, val string, z tinyint);
