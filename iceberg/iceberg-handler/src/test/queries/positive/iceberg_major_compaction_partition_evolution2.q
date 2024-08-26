-- SORT_QUERY_RESULTS
-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/
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
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/

set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.merge.ts.schema=true;

create table ice_orc (
    first_name string, 
    last_name string,
    dept_id bigint
 )
stored by iceberg stored as orc 
tblproperties ('format-version'='2');

insert into ice_orc VALUES ('fn1','ln1', 1);
insert into ice_orc VALUES ('fn2','ln2', 1);
insert into ice_orc VALUES ('fn3','ln3', 1);
insert into ice_orc VALUES ('fn4','ln4', 1);
delete from ice_orc where last_name in ('ln3', 'ln4');

alter table ice_orc set partition spec(dept_id);

insert into ice_orc PARTITION(dept_id=2) VALUES ('fn5','ln5');
insert into ice_orc PARTITION(dept_id=2) VALUES ('fn6','ln6');
insert into ice_orc PARTITION(dept_id=2) VALUES ('fn7','ln7');
insert into ice_orc PARTITION(dept_id=2) VALUES ('fn8','ln8');
delete from ice_orc where last_name in ('ln7', 'ln8');

select * from ice_orc;
describe formatted ice_orc;
show compactions;

alter table ice_orc COMPACT 'major' and wait;

select * from ice_orc;
describe formatted ice_orc;
show compactions;

