-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s\'current-snapshot-id\'=\')(\d+)(\')/$1#Masked#$3/
-- Mask added file size
--! qt:replace:/(\S+\"added-files-size\":\")(\d+)(\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S+\"total-files-size\":\")(\d+)(\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s\'current-snapshot-timestamp-ms\'=\')(\d+)(\')/$1#Masked#$3/

-- create an unpartitioned table with skip delete data set to false
 create table ice01 (id int) Stored by Iceberg stored as ORC
 TBLPROPERTIES('format-version'='2', 'iceberg.delete.skiprowdata'='false');

-- check the property value
show create table ice01;

-- insert some values
insert into ice01 values (1),(2),(3),(4);

-- check the inserted values
select * from ice01;

-- delete some values
delete from ice01 where id>2;

-- check the values, the delete value should be there
select * from ice01 order by id;

-- insert some more data
 insert into ice01 values (5),(6),(7),(8);

-- check the values, only the delete value shouldn't be there
select * from ice01 order by id;

-- delete one value
delete from ice01 where id=7;

-- change to skip the row data now
Alter table ice01 set TBLPROPERTIES('iceberg.delete.skiprowdata'='true');

-- check the property value
show create table ice01;

-- delete some more rows now
delete from ice01 where id=5;

-- check the entries, the deleted entries shouldn't be there.
select * from ice01 order by id;

-- create a partitioned table with skip row data set to false
 create table icepart01 (id int) partitioned by (part int) Stored by Iceberg stored as ORC
 TBLPROPERTIES('format-version'='2', 'iceberg.delete.skiprowdata'='false');

-- insert some values
insert into icepart01 values (1,1),(2,1),(3,2),(4,2);

-- check the inserted values
select * from icepart01 order by id;;

-- delete some values
delete from icepart01 where id>=2 AND id<4;

-- check the values, the delete value should be there
select * from icepart01;

-- insert some more data
 insert into icepart01 values (5,1),(6,2),(7,1),(8,2);

-- check the values, only the delete value shouldn't be there
select * from icepart01 order by id;

-- delete one value
delete from icepart01 where id=7;

-- change to skip the row data now
Alter table icepart01 set TBLPROPERTIES('iceberg.delete.skiprowdata'='true');

-- check the property value
show create table icepart01;

-- delete some more rows now
delete from icepart01 where id=5;

-- check the entries, the deleted entries shouldn't be there.
select * from icepart01 order by id;;

-- clean up
drop table ice01;
drop table icepart01;