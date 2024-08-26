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
    first_name string, 
    last_name string,
    registration_date date
 )
partitioned by (dept_id bigint, 
                city string)
stored by iceberg stored as orc 
tblproperties ('format-version'='2');

insert into ice_orc partition(dept_id=1, city='London') VALUES 
('fn1','ln1','2024-03-11'),
('fn2','ln2','2024-03-11');

insert into ice_orc partition(dept_id=1, city='London') VALUES 
('fn3','ln3','2024-03-11'),
('fn4','ln4','2024-03-11');

insert into ice_orc partition(dept_id=2, city='Paris') VALUES
('fn5','ln5','2024-02-16'),
('fn6','ln6','2024-02-16');

insert into ice_orc partition(dept_id=2, city='Paris') VALUES
('fn7','ln7','2024-02-16'),
('fn8','ln8','2024-02-16');

alter table ice_orc set partition spec(dept_id, city, registration_date);

insert into ice_orc partition(dept_id=1, city='London', registration_date='2024-03-11') VALUES 
('fn9','ln9'),
('fn10','ln10');

insert into ice_orc partition(dept_id=1, city='London', registration_date='2024-03-11') VALUES 
('fn11','ln11'),
('fn12','ln12');

insert into ice_orc partition(dept_id=2, city='Paris', registration_date='2024-02-16') VALUES
('fn13','ln13'),
('fn14','ln14');

insert into ice_orc partition(dept_id=2, city='Paris', registration_date='2024-02-16') VALUES
('fn15','ln15'),
('fn16','ln16');

delete from ice_orc where last_name in ('ln1', 'ln3', 'ln5', 'ln7', 'ln9', 'ln11', 'ln13', 'ln15');

select * from ice_orc;
describe formatted ice_orc;

explain alter table ice_orc PARTITION (dept_id=1, city='London', registration_date='2024-03-11') COMPACT 'major' and wait;
alter table ice_orc PARTITION (dept_id=1, city='London', registration_date='2024-03-11') COMPACT 'major' and wait;

select * from ice_orc;
describe formatted ice_orc;

explain alter table ice_orc PARTITION (dept_id=2, city='Paris', registration_date='2024-02-16') COMPACT 'major' and wait;
alter table ice_orc PARTITION (dept_id=2, city='Paris', registration_date='2024-02-16') COMPACT 'major' and wait;

select * from ice_orc;
describe formatted ice_orc;

explain alter table ice_orc PARTITION (dept_id=1, city='London') COMPACT 'major' and wait;
alter table ice_orc PARTITION (dept_id=1, city='London') COMPACT 'major' and wait;

select * from ice_orc;
describe formatted ice_orc;

explain alter table ice_orc PARTITION (dept_id=2, city='Paris') COMPACT 'major' and wait;
alter table ice_orc PARTITION (dept_id=2, city='Paris') COMPACT 'major' and wait;

select * from ice_orc;
describe formatted ice_orc;
show compactions;
