-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask the file size values as it can have slight variability, causing test flakiness
--! qt:replace:/("added-files-size":")\d+/$1#FileSize#/
--! qt:replace:/("total-files-size":")\d+/$1#FileSize#/
--! qt:replace:/("removed-files-size":")\d+/$1#FileSize#/

-- create an unpartitioned table with skip delete data set to false
set hive.cbo.fallback.strategy=NEVER;

 create table ice01 (id int, name string) Stored by Iceberg stored as ORC
 TBLPROPERTIES('format-version'='2');

-- insert some values
insert into ice01 values (1, 'ABC'),(2, 'CBS'),(3, null),(4, 'POPI'),(5, 'AQWR'),(6, 'POIU'),(9, null),(8,
'POIKL'),(10, 'YUIO');

-- delete using MOR

delete from ice01 where id>9 OR id=8;
select * from ice01;

-- should be 2 files, one data file and one positional delete file.
select summary from default.ice01.snapshots;

ALTER TABLE ice01 SET TBLPROPERTIES ('write.delete.mode'='copy-on-write');

-- delete some values
explain delete from ice01 where id>4 OR id=2;
delete from ice01 where id>4 OR id=2;

select * from ice01;

-- should be only one data file.
select summary from default.ice01.snapshots;

-- null cases

delete from ice01 where null;
select * from ice01;

delete from ice01 where not null;
select * from ice01;

delete from ice01 where name = null;
select * from ice01;

delete from ice01 where name != null;
select * from ice01;

--disable cbo due to HIVE-27070
set hive.cbo.enable=false;

delete from ice01 where name is null;
select * from ice01;

-- clean up
drop table if exists ice01;