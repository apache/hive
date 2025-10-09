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
-- Mask the enqueue time which is based on current time
--! qt:replace:/(MINOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
--! qt:replace:/(MINOR\s+refused\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
-- Mask compaction id as they will be allocated in parallel threads
--! qt:replace:/^[0-9]/#Masked#/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask iceberg version
--! qt:replace:/(\S\"iceberg-version\\\":\\\")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))(\\\")/$1#Masked#$3/

set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.merge.ts.schema=true;
set hive.merge.tezfiles=true;

create table ice_orc (
    first_name string, 
    last_name string
 )
stored by iceberg stored as orc 
tblproperties ('format-version'='2');

insert into ice_orc VALUES ('fn1','ln1');
insert into ice_orc VALUES ('fn5','ln5'), ('fn7','ln7'), ('fn4','ln4'), ('fn6','ln6');
insert into ice_orc VALUES ('fn3','ln3'), ('fn2','ln2');

-- File size threshold = fragment size = target_size/fragment_ratio = 2920/8 = 365.
alter table ice_orc set tblproperties ('compactor.threshold.target.size'='2920', 'compactor.threshold.min.input.files'='2');
explain alter table ice_orc COMPACT 'minor' and wait pool 'iceberg';
describe formatted ice_orc;

----------------------------------------------------------------------------------------------------------------
-- minor compaction with custom pool, filtering and results ordering
----------------------------------------------------------------------------------------------------------------

alter table ice_orc COMPACT 'minor' and wait pool 'iceberg' where first_name != 'Joe' order by first_name;

-- Expecting only those records to be ordered which reside in files that had been compacted
select * from ice_orc where first_name in ('fn1','fn2','fn3');
select * from ice_orc where first_name in ('fn4','fn5','fn6','fn7');
describe formatted ice_orc;
show compactions;

----------------------------------------------------------------------------------------------------------------
-- minor compaction with deletes
----------------------------------------------------------------------------------------------------------------

delete from ice_orc where first_name = 'fn7';
insert into ice_orc VALUES ('fn8','ln8');
alter table ice_orc COMPACT 'minor' and wait pool 'iceberg' order by first_name;

select * from ice_orc;
describe formatted ice_orc;
show compactions;