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

create table ice_orc (
    a string
 )
partitioned by (b bigint)
stored by iceberg stored as orc 
tblproperties ('format-version'='2');

insert into ice_orc partition(b=1) VALUES 
('a1'),
('a2'),
('a3');

insert into ice_orc partition(b=1) VALUES
('a4'),
('a5'),
('a6');

alter table ice_orc set partition spec(a);

insert into ice_orc partition (a='a') VALUES 
(1),
(2),
(3);

insert into ice_orc partition (a='a') VALUES 
(4),
(5),
(6);

select * from ice_orc;
describe formatted ice_orc;

delete from ice_orc where a in ('a2', 'a4');
delete from ice_orc where b in (3, 6);

select * from ice_orc;
describe formatted ice_orc;

explain alter table ice_orc partition(a='a') compact 'major' and wait;
alter table ice_orc partition(a='a') compact 'major' and wait;

select * from ice_orc;
describe formatted ice_orc;

explain alter table ice_orc partition(b=1) compact 'major' and wait;
alter table ice_orc partition(b=1) compact 'major' and wait;

select * from ice_orc;
describe formatted ice_orc;
