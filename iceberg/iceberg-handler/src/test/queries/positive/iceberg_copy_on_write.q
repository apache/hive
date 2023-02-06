-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask the file size values as it can have slight variability, causing test flakiness
--! qt:replace:/("added-files-size":")\d+/$1#FileSize#/
--! qt:replace:/("total-files-size":")\d+/$1#FileSize#/
--! qt:replace:/("removed-files-size":")\d+/$1#FileSize#/


-- create an unpartitioned table with skip delete data set to false
 create table ice01 (id int) Stored by Iceberg stored as ORC
 TBLPROPERTIES('format-version'='2');

-- insert some values
insert into ice01 values (1),(2),(3),(4),(5),(6),(9),(8),(10);

-- delete using MOR
delete from ice01 where id>9 OR id=8;

select * from ice01;

-- shoud be 2 files, one data file and one positional delete file.
select summary from default.ice01.snapshots;

ALTER TABLE ice01 SET TBLPROPERTIES ('write.delete.mode'='copy-on-write');

-- delete some values
delete from ice01 where id>4 OR id=2;

select * from ice01;

-- should be only one data file.
select summary from default.ice01.snapshots;

-- clean up
drop table if exists ice01;
drop table if exists icepart01;