
dfs -rmr -f  hdfs:///tmp/etp_1;

dfs -mkdir -p  hdfs:///tmp/etp_1;
dfs -copyFromLocal ../../data/files/kv1.txt hdfs:///tmp/etp_1/;

create external table etp_1 (c1 string, c2 string) stored as textfile location 'hdfs:///tmp/etp_1';
set test.comment=Table should have data;
set test.comment;
select count(*) from etp_1;

drop table etp_1;

-- Create external table in same location, data should still be there
create external table etp_1 (c1 string, c2 string) stored as textfile location 'hdfs:///tmp/etp_1';
set test.comment=Table should have data;
set test.comment;
select count(*) from etp_1;
alter table etp_1 set tblproperties ('external.table.purge'='true');

drop table etp_1;

-- Create external table in same location. Data should be gone due to external.table.purge option.
create external table etp_1 (c1 string, c2 string) stored as textfile location 'hdfs:///tmp/etp_1';
set test.comment=Table should have no data;
set test.comment;
select count(*) from etp_1;

drop table etp_1;

--
-- Test hive.external.table.purge.default
--

dfs -mkdir -p  hdfs:///tmp/etp_1;
dfs -copyFromLocal ../../data/files/kv1.txt hdfs:///tmp/etp_1/;

set hive.external.table.purge.default=true;

-- Can still create table and override the default
create external table etp_1 (c1 string, c2 string) stored as textfile location 'hdfs:///tmp/etp_1' tblproperties ('external.table.purge'='false');
show create table etp_1;
set test.comment=Table should have data;
set test.comment;
select count(*) from etp_1;

drop table etp_1;

-- Create with default options, external.table.purge should be set
create external table etp_1 (c1 string, c2 string) stored as textfile location 'hdfs:///tmp/etp_1';
show create table etp_1;
set test.comment=Table should have data;
set test.comment;
select count(*) from etp_1;

drop table etp_1;

-- Data should be gone
create external table etp_1 (c1 string, c2 string) stored as textfile location 'hdfs:///tmp/etp_1';
set test.comment=Table should have no data;
set test.comment;
select count(*) from etp_1;

drop table etp_1;
dfs -rmr -f hdfs:///tmp/etp_1;

set hive.external.table.purge.default=false;

--
-- Partitioned table
--

dfs -rmr -f  hdfs:///tmp/etp_2;
dfs -mkdir -p  hdfs:///tmp/etp_2/p1=part1;
dfs -mkdir -p  hdfs:///tmp/etp_2/p1=part2;
dfs -copyFromLocal ../../data/files/kv1.txt hdfs:///tmp/etp_2/p1=part1/;
dfs -copyFromLocal ../../data/files/kv1.txt hdfs:///tmp/etp_2/p1=part2/;

create external table etp_2 (c1 string, c2 string) partitioned by (p1 string) stored as textfile location 'hdfs:///tmp/etp_2';
alter table etp_2 add partition (p1='part1');
alter table etp_2 add partition (p1='part2');
set test.comment=Table should have full data;
set test.comment;
select count(*) from etp_2;
alter table etp_2 drop partition (p1='part1');
alter table etp_2 add partition (p1='part1');
set test.comment=Table should have full data;
set test.comment;
select count(*) from etp_2;

drop table etp_2;

-- Create external table in same location, data should still be there
create external table etp_2 (c1 string, c2 string) partitioned by (p1 string) stored as textfile location 'hdfs:///tmp/etp_2';
alter table etp_2 set tblproperties ('external.table.purge'='true');
alter table etp_2 add partition (p1='part1');
alter table etp_2 add partition (p1='part2');
set test.comment=Table should have full data;
set test.comment;
select count(*) from etp_2;
alter table etp_2 drop partition (p1='part1');
alter table etp_2 add partition (p1='part1');
set test.comment=Table should have partial data;
set test.comment;
select count(*) from etp_2;

drop table etp_2;

-- Create external table in same location. Data should be gone due to external.table.purge option.
create external table etp_2 (c1 string, c2 string) partitioned by (p1 string) stored as textfile location 'hdfs:///tmp/etp_2';
alter table etp_2 add partition (p1='part1');
alter table etp_2 add partition (p1='part2');
set test.comment=Table should have no data;
set test.comment;
select count(*) from etp_2;

drop table etp_2;

-- Test hive.external.table.purge.default
dfs -mkdir -p  hdfs:///tmp/etp_2/p1=part1;
dfs -mkdir -p  hdfs:///tmp/etp_2/p1=part2;
dfs -copyFromLocal ../../data/files/kv1.txt hdfs:///tmp/etp_2/p1=part1/;
dfs -copyFromLocal ../../data/files/kv1.txt hdfs:///tmp/etp_2/p1=part2/;

set hive.external.table.purge.default=true;

-- Can still create table and override the default
create external table etp_2 (c1 string, c2 string) partitioned by (p1 string) stored as textfile location 'hdfs:///tmp/etp_2' tblproperties ('external.table.purge'='false');
show create table etp_2;
alter table etp_2 add partition (p1='part1');
alter table etp_2 add partition (p1='part2');
set test.comment=Table should have full data;
set test.comment;
select count(*) from etp_2;

drop table etp_2;

-- Create with default options, external.table.purge should be set
create external table etp_2 (c1 string, c2 string) partitioned by (p1 string) stored as textfile location 'hdfs:///tmp/etp_2';
show create table etp_2;
alter table etp_2 add partition (p1='part1');
alter table etp_2 add partition (p1='part2');
set test.comment=Table should have full data;
set test.comment;
select count(*) from etp_2;
alter table etp_2 drop partition (p1='part1');
alter table etp_2 add partition (p1='part1');
set test.comment=Table should have partial data;
set test.comment;
select count(*) from etp_2;

drop table etp_2;

-- Data should be gone
create external table etp_2 (c1 string, c2 string) partitioned by (p1 string) stored as textfile location 'hdfs:///tmp/etp_2';
alter table etp_2 add partition (p1='part1');
alter table etp_2 add partition (p1='part2');
set test.comment=Table should have no data;
set test.comment;
select count(*) from etp_2;

drop table etp_2;
dfs -rmr -f hdfs:///tmp/etp_2;

set hive.external.table.purge.default=false;
