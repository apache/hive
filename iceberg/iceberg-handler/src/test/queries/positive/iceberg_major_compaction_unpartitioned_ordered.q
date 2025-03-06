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
--! qt:replace:/(MAJOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
-- Mask compaction id as they will be allocated in parallel threads
--! qt:replace:/^[0-9]/#Masked#/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask iceberg version
--! qt:replace:/(\S\"iceberg-version\\\":\\\")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))(\\\")/$1#Masked#$3/

set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.merge.ts.schema=true;

create table ice_orc (
    first_name string, 
    last_name string
 )
stored by iceberg stored as orc 
tblproperties ('format-version'='2', 'hive.compactor.worker.pool'='iceberg', 'compactor.threshold.target.size'='1500');

insert into ice_orc VALUES 
('fn1','ln1'),
('fn2','ln2'),
('fn3','ln3'),
('fn4','ln4'),
('fn5','ln5'),
('fn6','ln6'),
('fn7','ln7');

delete from ice_orc where last_name in ('ln5', 'ln6', 'ln7');

select * from ice_orc;
describe formatted ice_orc;

explain alter table ice_orc COMPACT 'major' and wait order by last_name desc;
explain optimize table ice_orc rewrite data order by last_name desc;

alter table ice_orc COMPACT 'major' and wait order by last_name desc;

select * from ice_orc;
describe formatted ice_orc;
show compactions;