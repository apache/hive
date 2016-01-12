SET hive.exec.schema.evolution=true;
create table src_orc (key tinyint, val string) stored as orc;
alter table src_orc change key k tinyint after val;
