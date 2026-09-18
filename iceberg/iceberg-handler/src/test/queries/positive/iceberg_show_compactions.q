-- Mask show compactions fields that change across runs
--! qt:replace:/^[0-9]/#Masked#/
--! qt:replace:/(MAJOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/

-- Test compaction entry is removed upon drop non-partitioned table
create table ice_t1 (i int) stored by iceberg tblproperties ('compactor.threshold.target.size'='1500');
insert into ice_t1 values(1),(2);
insert into ice_t1 values(3),(4);
alter table ice_t1 compact 'major' and wait;
show compactions ice_t1;
drop table ice_t1;
show compactions ice_t1;

-- Test compaction entry is updated upon rename non-partitioned table
create table ice_t2 (i int) stored by iceberg tblproperties ('compactor.threshold.target.size'='1500');
insert into ice_t2 values(1),(2);
insert into ice_t2 values(3),(4);
alter table ice_t2 compact 'major' and wait;
show compactions ice_t2;
alter table ice_t2 RENAME to ice_t2_new;
show compactions ice_t2_new;
drop table ice_t2_new;

-- Test compaction entries are removed upon drop partition and drop table for a partitioned table
create table ice_part (i int) partitioned by (j int) stored by iceberg tblproperties ('compactor.threshold.target.size'='1500');
insert into ice_part values (1,1);
insert into ice_part values (2,1);
alter table ice_part partition (j=1) compact 'major' and wait;
insert into ice_part values (1,2);
insert into ice_part values (2,2);
alter table ice_part partition (j=2) compact 'major' and wait;
show compactions ice_part;
alter table ice_part drop partition (j=1);
show compactions ice_part;
drop table ice_part;
show compactions ice_part;