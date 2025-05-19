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
--! qt:replace:/(MAJOR\s+refused\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
--! qt:replace:/(SMART_OPTIMIZE\s+refused\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
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
    last_name string,
    dept_id bigint,
    team_id bigint
 )
partitioned by (company_id bigint)
stored by iceberg stored as orc 
tblproperties ('format-version'='2', 'compactor.threshold.target.size'='1500');

insert into ice_orc VALUES 
('fn1','ln1', 1, 10, 100),
('fn2','ln2', 1, 10, 100),
('fn3','ln3', 1, 11, 100),
('fn4','ln4', 1, 11, 100);

alter table ice_orc set partition spec(dept_id);

insert into ice_orc VALUES 
('fn5','ln5', 2, 20, 101),
('fn6','ln6', 2, 20, 101);

insert into ice_orc VALUES 
('fn7','ln7', 2, 20, 101),
('fn8','ln8', 2, 20, 101);

insert into ice_orc VALUES 
('fn9', 'ln9',  3, 20, 101),
('fn10','ln10', 3, 20, 101),
('fn11','ln11', 3, 20, 101),
('fn12','ln12', 3, 20, 101);

select * from ice_orc where company_id = 100;
select * from ice_orc where dept_id = 2 order by first_name;
select * from ice_orc where dept_id = 3 order by first_name;
describe formatted ice_orc;

explain alter table ice_orc COMPACT 'smart_optimize' and wait order by first_name desc;
explain optimize table ice_orc rewrite data order by first_name desc;

alter table ice_orc COMPACT 'smart_optimize' and wait order by first_name desc;

select * from ice_orc where company_id = 100;
select * from ice_orc where dept_id = 2;
select * from ice_orc where dept_id = 3;

describe formatted ice_orc;
show compactions order by 'partition';