SET hive.exec.schema.evolution=true;
create table src_orc (key int, val string) stored as orc;
insert into src_orc values (1, "Alice");
alter table src_orc drop column val;
