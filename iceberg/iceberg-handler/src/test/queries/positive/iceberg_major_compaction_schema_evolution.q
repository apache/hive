-- SORT_QUERY_RESULTS
-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/
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
--! qt:replace:/(MAJOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
-- Mask compaction id as they will be allocated in parallel threads
--! qt:replace:/^[0-9]/#Masked#/

set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.merge.ts.schema=true;

create table ice_orc (
    first_name string, 
    last_name string
 )
partitioned by (dept_id bigint)
stored by iceberg stored as orc 
tblproperties ('format-version'='2');

insert into ice_orc (first_name, last_name, dept_id) VALUES ('fn1','ln1', 1);
insert into ice_orc (first_name, last_name, dept_id) VALUES ('fn2','ln2', 1);
insert into ice_orc (first_name, last_name, dept_id) VALUES ('fn3','ln3', 1);
insert into ice_orc (first_name, last_name, dept_id) VALUES ('fn4','ln4', 1);

alter table ice_orc add columns (address string);

insert into ice_orc (first_name, last_name, address, dept_id) VALUES ('fn5','ln5', 'addr_5', 2);
insert into ice_orc (first_name, last_name, address, dept_id) VALUES ('fn6','ln6', 'addr_6', 2);
insert into ice_orc (first_name, last_name, address, dept_id) VALUES ('fn7','ln7', 'addr_7', 2);
insert into ice_orc (first_name, last_name, address, dept_id) VALUES ('fn8','ln8', 'addr_8', 2);

update ice_orc set last_name = 'ln1a' where first_name='fn1';
update ice_orc set last_name = 'ln2a' where first_name='fn2';
update ice_orc set last_name = 'ln3a' where first_name='fn3';
update ice_orc set last_name = 'ln4a' where first_name='fn4';

alter table ice_orc change first_name fname string;

update ice_orc set last_name = 'ln5a' where fname='fn5';
update ice_orc set last_name = 'ln6a' where fname='fn6';
update ice_orc set last_name = 'ln7a' where fname='fn7';
update ice_orc set last_name = 'ln8a' where fname='fn8';

delete from ice_orc where fname in ('fn1', 'fn3', 'fn7');

select * from ice_orc;
describe formatted ice_orc;

explain alter table ice_orc COMPACT 'major' and wait;
alter table ice_orc COMPACT 'major' and wait;

select * from ice_orc;
describe formatted ice_orc;
show compactions;