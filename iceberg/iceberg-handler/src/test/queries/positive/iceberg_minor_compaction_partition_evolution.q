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

create database ice_comp with dbproperties('hive.compactor.worker.pool'='iceberg');
use ice_comp;
 
create table ice_orc (
    first_name string, 
    last_name string,
    dept_id bigint
 )
stored by iceberg stored as orc 
tblproperties ('format-version'='2');

insert into ice_orc VALUES ('fn2','ln2', 1), ('fn1','ln1', 1);
insert into ice_orc VALUES ('fn4','ln4', 1), ('fn3','ln3', 1);

delete from ice_orc where last_name in ('ln4');

alter table ice_orc set partition spec(dept_id);

insert into ice_orc PARTITION(dept_id=2) VALUES ('fn6','ln6'), ('fn5','ln5');
insert into ice_orc PARTITION(dept_id=2) VALUES ('fn8','ln8'), ('fn7','ln7');

delete from ice_orc where last_name in ('ln8');

describe formatted ice_orc;
show compactions order by 'partition';

-- File size threshold = fragment size = target_size/fragment_ratio = 3200/8 = 400.
-- Size of every data file in bytes is 463, therefore nothing to compact.
alter table ice_orc set tblproperties ('compactor.threshold.target.size'='3200', 'compactor.threshold.min.input.files'='4');
alter table ice_orc COMPACT 'minor' and wait pool 'iceberg';

describe formatted ice_orc;
show compactions order by 'partition';

-- File size threshold = fragment size = target_size/fragment_ratio = 4000/8 = 500.
-- Now data files' sizes are below file_size_threshold, therefore compaction is needed.
alter table ice_orc set tblproperties ('compactor.threshold.target.size'='4000');
alter table ice_orc COMPACT 'minor' and wait pool 'iceberg' where fist_name in ('fn1','fn2','fn5','fn6') order by first_name;;

select * from ice_orc where dept_id=1;
select * from ice_orc where dept_id=2;
describe formatted ice_orc;
show compactions order by 'partition';
