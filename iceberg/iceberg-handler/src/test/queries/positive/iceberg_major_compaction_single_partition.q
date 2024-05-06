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
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
--! qt:replace:/(MAJOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
-- Mask compaction id as they will be allocated in parallel threads
--! qt:replace:/^[0-9]/#Masked#/

set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.merge.ts.schema=true;

create table ice_orc_wo_evo (
    first_name string, 
    last_name string
 )
partitioned by (dept_id bigint, 
                city string, 
                registration_date date)
stored by iceberg stored as orc 
tblproperties ('format-version'='2');

insert into ice_orc_wo_evo VALUES ('fn1','ln1',1,'London','2024-03-11');
insert into ice_orc_wo_evo VALUES ('fn2','ln2',1,'London','2024-03-11');
insert into ice_orc_wo_evo VALUES ('fn3','ln3',1,'London','2024-03-11');
insert into ice_orc_wo_evo VALUES ('fn4','ln4',1,'London','2024-03-11');
insert into ice_orc_wo_evo VALUES ('fn5','ln5',2,'Paris','2024-02-16');
insert into ice_orc_wo_evo VALUES ('fn6','ln6',2,'Paris','2024-02-16');
insert into ice_orc_wo_evo VALUES ('fn7','ln7',2,'Paris','2024-02-16');

update ice_orc_wo_evo set last_name = 'ln1a' where first_name='fn1';
update ice_orc_wo_evo set last_name = 'ln2a' where first_name='fn2';
update ice_orc_wo_evo set last_name = 'ln3a' where first_name='fn3';
update ice_orc_wo_evo set last_name = 'ln4a' where first_name='fn4';
update ice_orc_wo_evo set last_name = 'ln5a' where first_name='fn5';
update ice_orc_wo_evo set last_name = 'ln6a' where first_name='fn6';
update ice_orc_wo_evo set last_name = 'ln7a' where first_name='fn7';

delete from ice_orc_wo_evo where last_name in ('ln1a', 'ln2a', 'ln7a');

select * from ice_orc_wo_evo;
describe formatted ice_orc_wo_evo;

explain alter table ice_orc_wo_evo PARTITION (dept_id=1, city='London', registration_date='2024-03-11') COMPACT 'major' and wait;
alter table ice_orc_wo_evo PARTITION (dept_id=1, city='London', registration_date='2024-03-11') COMPACT 'major' and wait;

select * from ice_orc_wo_evo;
describe formatted ice_orc_wo_evo;
show compactions;

explain alter table ice_orc_wo_evo PARTITION (dept_id=2, city='Paris', registration_date='2024-02-16') COMPACT 'major' and wait;
alter table ice_orc_wo_evo PARTITION (dept_id=2, city='Paris', registration_date='2024-02-16') COMPACT 'major' and wait;

select * from ice_orc_wo_evo;
describe formatted ice_orc_wo_evo;
show compactions;
