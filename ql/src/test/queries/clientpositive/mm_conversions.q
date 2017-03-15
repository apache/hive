set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.exec.dynamic.partition.mode=nonstrict;


-- Force multiple writers when reading
drop table intermediate;
create table intermediate(key int) partitioned by (p int) stored as orc;
insert into table intermediate partition(p='455') select distinct key from src where key >= 0 order by key desc limit 1;
insert into table intermediate partition(p='456') select distinct key from src where key is not null order by key asc limit 1;
insert into table intermediate partition(p='457') select distinct key from src where key >= 100 order by key asc limit 1;

drop table simple_from_mm;
create table simple_from_mm(key int) stored as orc tblproperties ("transactional"="true", "transactional_properties"="insert_only");
insert into table simple_from_mm select key from intermediate;
insert into table simple_from_mm select key from intermediate;
select * from simple_from_mm s1 order by key;
alter table simple_from_mm unset tblproperties('transactional_properties', 'transactional');
select * from simple_from_mm s2 order by key;
insert into table simple_from_mm select key from intermediate;
select * from simple_from_mm s3 order by key;
alter table simple_from_mm set tblproperties("transactional"="true", "transactional_properties"="insert_only");
select * from simple_from_mm s4 order by key;
insert into table simple_from_mm select key from intermediate;
select * from simple_from_mm s5 order by key;
alter table simple_from_mm set tblproperties("transactional"="false", 'transactional_properties'='false');
select * from simple_from_mm s6 order by key;
insert into table simple_from_mm select key from intermediate;
select * from simple_from_mm s7 order by key;
drop table simple_from_mm;

drop table simple_to_mm;
create table simple_to_mm(key int) stored as orc;
insert into table simple_to_mm select key from intermediate;
insert into table simple_to_mm select key from intermediate;
select * from simple_to_mm s1 order by key;
alter table simple_to_mm set tblproperties("transactional"="true", "transactional_properties"="insert_only");
select * from simple_to_mm s2 order by key;
insert into table simple_to_mm select key from intermediate;
insert into table simple_to_mm select key from intermediate;
select * from simple_to_mm s3 order by key;
drop table simple_to_mm;

drop table part_from_mm;
create table part_from_mm(key int) partitioned by (key_mm int) stored as orc tblproperties ("transactional"="true", "transactional_properties"="insert_only");
insert into table part_from_mm partition(key_mm='455') select key from intermediate;
insert into table part_from_mm partition(key_mm='455') select key from intermediate;
insert into table part_from_mm partition(key_mm='456') select key from intermediate;
select * from part_from_mm s1 order by key, key_mm;
alter table part_from_mm unset tblproperties('transactional_properties', 'transactional');
select * from part_from_mm s2 order by key, key_mm;
insert into table part_from_mm partition(key_mm='456') select key from intermediate;
insert into table part_from_mm partition(key_mm='457') select key from intermediate;
select * from part_from_mm s3 order by key, key_mm;
alter table part_from_mm set tblproperties("transactional"="true", "transactional_properties"="insert_only");
select * from part_from_mm s4 order by key, key_mm;
insert into table part_from_mm partition(key_mm='456') select key from intermediate;
insert into table part_from_mm partition(key_mm='455') select key from intermediate;
select * from part_from_mm s5 order by key, key_mm;
alter table part_from_mm set tblproperties("transactional"="false", 'transactional_properties'='false');
select * from part_from_mm s6 order by key, key_mm;
insert into table part_from_mm partition(key_mm='457') select key from intermediate;
select * from part_from_mm s7 order by key, key_mm;
drop table part_from_mm;

drop table part_to_mm;
create table part_to_mm(key int) partitioned by (key_mm int) stored as orc;
insert into table part_to_mm partition(key_mm='455') select key from intermediate;
insert into table part_to_mm partition(key_mm='456') select key from intermediate;
select * from part_to_mm s1 order by key, key_mm;
alter table part_to_mm set tblproperties("transactional"="true", "transactional_properties"="insert_only");
select * from part_to_mm s2 order by key, key_mm;
insert into table part_to_mm partition(key_mm='456') select key from intermediate;
insert into table part_to_mm partition(key_mm='457') select key from intermediate;
select * from part_to_mm s3 order by key, key_mm;
drop table part_to_mm;

drop table intermediate;
