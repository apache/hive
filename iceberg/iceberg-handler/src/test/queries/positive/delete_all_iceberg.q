-- SORT_QUERY_RESULTS
-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/

set hive.vectorized.execution.enabled=true;

create table ice01 (id int, key int) Stored by Iceberg stored as ORC 
  TBLPROPERTIES('format-version'='2', 'iceberg.delete.skiprowdata'='false');

insert into ice01 values (1,1),(2,1),(3,1),(4,1);
insert into ice01 values (1,2),(2,2),(3,2),(4,2);
insert into ice01 values (1,3),(2,3),(3,3),(4,3);
insert into ice01 values (1,4),(2,4),(3,4),(4,4);
insert into ice01 values (1,5),(2,5),(3,5),(4,5);

set hive.explain.user=false;
explain analyze delete from ice01;

delete from ice01;

select count(*) from ice01;
select * from ice01;
describe formatted ice01;

drop table ice01;

-- Create a V2 table with Copy-on-write as the deletion mode.
create table ice01 (id int, key int) stored by iceberg stored as orc tblproperties ('format-version'='2', 'write.delete.mode'='copy-on-write');

insert into ice01 values (1,1),(2,1),(3,1),(4,1);
insert into ice01 values (1,2),(2,2),(3,2),(4,2);
insert into ice01 values (1,3),(2,3),(3,3),(4,3);
insert into ice01 values (1,4),(2,4),(3,4),(4,4);
insert into ice01 values (1,5),(2,5),(3,5),(4,5);

explain analyze delete from ice01;

delete from ice01;

select count(*) from ice01;
select * from ice01;
describe formatted ice01;
drop table ice01;

-- Create a V1 table.
create table ice01 (id int, key int) stored by iceberg stored as orc;

insert into ice01 values (1,1),(2,1),(3,1),(4,1);
insert into ice01 values (1,2),(2,2),(3,2),(4,2);
insert into ice01 values (1,3),(2,3),(3,3),(4,3);
insert into ice01 values (1,4),(2,4),(3,4),(4,4);
insert into ice01 values (1,5),(2,5),(3,5),(4,5);

explain analyze delete from ice01;

-- Perform delete on the V1 table
delete from ice01;

select count(*) from ice01;
select * from ice01;
describe formatted ice01;
drop table ice01;
