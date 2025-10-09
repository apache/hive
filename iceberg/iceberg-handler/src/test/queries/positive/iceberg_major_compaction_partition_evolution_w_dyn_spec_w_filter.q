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
    event_id int, 
    event_time timestamp with local time zone,
    event_src string    
)
partitioned by spec(truncate(3, event_src))
stored by iceberg stored as orc
tblproperties ('compactor.threshold.target.size'='1500');

insert into ice_orc values 
    (1, cast('2023-07-20 00:00:00' as timestamp with local time zone), 'AAA_1'),
    (2, cast('2023-07-21 00:00:00' as timestamp with local time zone), 'AAA_2');
insert into ice_orc values 
    (3, cast('2023-07-23 00:00:00' as timestamp with local time zone), 'AAA_1'),
    (4, cast('2023-07-24 00:00:00' as timestamp with local time zone), 'AAA_2');
insert into ice_orc values 
    (5, cast('2023-08-04 00:00:00' as timestamp with local time zone), 'BBB_1'),
    (6, cast('2023-08-05 00:00:00' as timestamp with local time zone), 'BBB_2');
insert into ice_orc values 
    (7, cast('2023-08-06 00:00:00' as timestamp with local time zone), 'BBB_1'),
    (8, cast('2023-08-27 00:00:00' as timestamp with local time zone), 'BBB_2');
    
alter table ice_orc set partition spec(truncate(3, event_src), month(event_time));

insert into ice_orc values 
    (9, cast('2024-07-20 00:00:00' as timestamp with local time zone), 'AAA_1'),
    (10, cast('2024-07-21 00:00:00' as timestamp with local time zone), 'AAA_2');
insert into ice_orc values     
    (11, cast('2024-08-22 00:00:00' as timestamp with local time zone), 'AAA_1'),
    (12, cast('2024-08-23 00:00:00' as timestamp with local time zone), 'AAA_2');
insert into ice_orc values     
    (13, cast('2024-08-24 00:00:00' as timestamp with local time zone), 'AAA_1'),
    (14, cast('2024-08-25 00:00:00' as timestamp with local time zone), 'AAA_2');
insert into ice_orc values 
    (15, cast('2024-09-24 00:00:00' as timestamp with local time zone), 'AAA_1'),
    (16, cast('2024-09-25 00:00:00' as timestamp with local time zone), 'AAA_2');
insert into ice_orc values 
    (17, cast('2024-07-01 00:00:00' as timestamp with local time zone), 'BBB_1'),
    (18, cast('2024-07-02 00:00:00' as timestamp with local time zone), 'BBB_2');
insert into ice_orc values 
    (19, cast('2024-07-03 00:00:00' as timestamp with local time zone), 'BBB_1'),
    (20, cast('2024-07-04 00:00:00' as timestamp with local time zone), 'BBB_2');
insert into ice_orc values 
    (21, cast('2024-08-03 00:00:00' as timestamp with local time zone), 'BBB_1'),
    (22, cast('2024-08-04 00:00:00' as timestamp with local time zone), 'BBB_2');
insert into ice_orc values 
    (23, cast('2024-08-05 00:00:00' as timestamp with local time zone), 'BBB_1'),
    (24, cast('2024-08-06 00:00:00' as timestamp with local time zone), 'BBB_2');
insert into ice_orc values
    (25, cast('2024-09-05 00:00:00' as timestamp with local time zone), 'BBB_1'),
    (26, cast('2024-09-06 00:00:00' as timestamp with local time zone), 'BBB_2');

select * from ice_orc;
describe formatted ice_orc;

alter table ice_orc COMPACT 'major' and wait 
where (event_src in ('BBB_1', 'BBB_2') and event_time < '2024-09-01 00:00:00') or
      (event_src in ('AAA_1', 'AAA_2') and event_time > '2024-08-01 00:00:00');

select * from ice_orc;
describe formatted ice_orc;

show compactions order by 'partition';