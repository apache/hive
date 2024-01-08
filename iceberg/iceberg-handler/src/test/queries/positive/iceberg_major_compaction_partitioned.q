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

insert into ice_orc VALUES ('fn1','ln1', 1);
insert into ice_orc VALUES ('fn2','ln2', 1);
insert into ice_orc VALUES ('fn3','ln3', 1);
insert into ice_orc VALUES ('fn4','ln4', 1);
insert into ice_orc VALUES ('fn5','ln5', 2);
insert into ice_orc VALUES ('fn6','ln6', 2);
insert into ice_orc VALUES ('fn7','ln7', 2);

update ice_orc set last_name = 'ln1a' where first_name='fn1';
update ice_orc set last_name = 'ln2a' where first_name='fn2';
update ice_orc set last_name = 'ln3a' where first_name='fn3';
update ice_orc set last_name = 'ln4a' where first_name='fn4';
update ice_orc set last_name = 'ln5a' where first_name='fn5';
update ice_orc set last_name = 'ln6a' where first_name='fn6';
update ice_orc set last_name = 'ln7a' where first_name='fn7';

delete from ice_orc where last_name in ('ln1a', 'ln2a', 'ln7a');

select * from ice_orc;
describe formatted ice_orc;

explain alter table ice_orc COMPACT 'major' and wait;
alter table ice_orc COMPACT 'major' and wait;

select * from ice_orc;
describe formatted ice_orc;
show compactions;

-- Starting second set of inserts/updates/deletes and calling compaction at the end
-- to check that subsequent compaction works

insert into ice_orc VALUES ('fn11','ln11', 1);
insert into ice_orc VALUES ('fn12','ln12', 1);
insert into ice_orc VALUES ('fn13','ln13', 1);
insert into ice_orc VALUES ('fn14','ln14', 1);
insert into ice_orc VALUES ('fn15','ln15', 2);
insert into ice_orc VALUES ('fn16','ln16', 2);
insert into ice_orc VALUES ('fn17','ln17', 2);
insert into ice_orc VALUES ('fn18','ln18', 2);

update ice_orc set last_name = 'ln11a' where first_name='fn11';
update ice_orc set last_name = 'ln12a' where first_name='fn12';
update ice_orc set last_name = 'ln13a' where first_name='fn13';
update ice_orc set last_name = 'ln14a' where first_name='fn14';
update ice_orc set last_name = 'ln15a' where first_name='fn15';
update ice_orc set last_name = 'ln16a' where first_name='fn16';
update ice_orc set last_name = 'ln17a' where first_name='fn17';
update ice_orc set last_name = 'ln18a' where first_name='fn18';

delete from ice_orc where last_name in ('ln11a', 'ln12a', 'ln17a', 'ln18a');

select * from ice_orc;
describe formatted ice_orc;

explain alter table ice_orc COMPACT 'major' and wait;
alter table ice_orc COMPACT 'major' and wait;

select * from ice_orc;
describe formatted ice_orc;
show compactions;
