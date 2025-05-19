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
--! qt:replace:/(MAJOR\s+refused\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
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
tblproperties ('format-version'='2', 'hive.compactor.worker.pool'='iceberg','compactor.threshold.target.size'='1500');

insert into ice_orc VALUES ('fn1','ln1', 1, 10, 100);
insert into ice_orc VALUES ('fn2','ln2', 1, 10, 100);
insert into ice_orc VALUES ('fn3','ln3', 1, 11, 100);
insert into ice_orc VALUES (null,null, null, null, null);
alter table ice_orc create tag v1;

alter table ice_orc set partition spec(company_id, dept_id);
insert into ice_orc VALUES ('fn4','ln4', 1, 11, 100);
insert into ice_orc VALUES ('fn5','ln5', 2, 20, 100);
insert into ice_orc VALUES ('fn6','ln6', 2, 20, 100);
insert into ice_orc VALUES (null,null, null, null, null);
alter table ice_orc create tag v2;

alter table ice_orc set partition spec(company_id, dept_id, team_id);
insert into ice_orc VALUES ('fn7','ln7', 2, 21, 100);
insert into ice_orc VALUES ('fn8','ln8', 2, 21, 100);
insert into ice_orc VALUES (null,null, null, null, null);

update ice_orc set last_name = 'ln1a' where first_name='fn1';
update ice_orc set last_name = 'ln2a' where first_name='fn2';
update ice_orc set last_name = 'ln3a' where first_name='fn3';
update ice_orc set last_name = 'ln4a' where first_name='fn4';
alter table ice_orc create tag v3;

alter table ice_orc set partition spec(company_id, dept_id);
update ice_orc set last_name = 'ln5a' where first_name='fn5';
update ice_orc set last_name = 'ln6a' where first_name='fn6';
update ice_orc set last_name = 'ln7a' where first_name='fn7';
update ice_orc set last_name = 'ln8a' where first_name='fn8';

delete from ice_orc where last_name in ('ln1a', 'ln8a');
alter table ice_orc create tag v4;

select * from ice_orc;
describe formatted ice_orc;

select `partition`, spec_id, content, record_count
from default.ice_orc.files
order by `partition`, spec_id, content, record_count;

-- Disable fetch tasks to see statistics
set hive.fetch.task.conversion=none;
explain select * from default.ice_orc.tag_v1;
explain select * from default.ice_orc.tag_v2;
explain select * from default.ice_orc.tag_v3;
explain select * from default.ice_orc.tag_v4;
explain select * from ice_orc;

explain select * from default.ice_orc.tag_v1 where company_id is not null;
explain select * from default.ice_orc.tag_v2 where company_id is not null;
explain select * from default.ice_orc.tag_v3 where company_id is not null;
explain select * from default.ice_orc.tag_v4 where company_id is not null;
explain select * from ice_orc where company_id is not null;
set hive.fetch.task.conversion=more;

explain alter table ice_orc COMPACT 'major' and wait;
alter table ice_orc COMPACT 'major' and wait;

select * from ice_orc;
describe formatted ice_orc;
show compactions order by 'partition';

select `partition`, spec_id, content, record_count
from default.ice_orc.files
order by `partition`, spec_id, content, record_count;

set hive.fetch.task.conversion=none;
explain select * from ice_orc;
explain select * from ice_orc where company_id is not null;
