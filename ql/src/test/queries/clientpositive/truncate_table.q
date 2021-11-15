create table src_truncate (key string, value string);
load data local inpath '../../data/files/kv1.txt' into table src_truncate;

create table src_truncate_alt (key string, value string);
load data local inpath '../../data/files/kv1.txt' into table src_truncate_alt;

create table srcpart_truncate (key string, value string) partitioned by (ds string, hr string);
alter table srcpart_truncate add partition (ds='2008-04-08', hr='11');        
alter table srcpart_truncate add partition (ds='2008-04-08', hr='12');
alter table srcpart_truncate add partition (ds='2008-04-09', hr='11');
alter table srcpart_truncate add partition (ds='2008-04-09', hr='12');

load data local inpath '../../data/files/kv1.txt' into table srcpart_truncate partition (ds='2008-04-08', hr='11');
load data local inpath '../../data/files/kv1.txt' into table srcpart_truncate partition (ds='2008-04-08', hr='12');
load data local inpath '../../data/files/kv1.txt' into table srcpart_truncate partition (ds='2008-04-09', hr='11');
load data local inpath '../../data/files/kv1.txt' into table srcpart_truncate partition (ds='2008-04-09', hr='12');

analyze table src_truncate     compute statistics;
analyze table srcpart_truncate partition(ds,hr) compute statistics;
set hive.fetch.task.conversion=more;
set hive.compute.query.using.stats=true;

-- truncate non-partitioned table
explain TRUNCATE TABLE src_truncate;
TRUNCATE TABLE src_truncate;
select * from src_truncate;
select count (*) from src_truncate;

-- truncate non-partitioned table with alternative syntax
explain TRUNCATE src_truncate_alt;
TRUNCATE src_truncate_alt;
select * from src_truncate_alt;
select count (*) from src_truncate_alt;

-- truncate a partition
explain TRUNCATE TABLE srcpart_truncate partition (ds='2008-04-08', hr='11');
TRUNCATE TABLE srcpart_truncate partition (ds='2008-04-08', hr='11');
select * from srcpart_truncate where ds='2008-04-08' and hr='11';
select count(*) from srcpart_truncate where ds='2008-04-08' and hr='11';

-- truncate partitions with partial spec
explain TRUNCATE TABLE srcpart_truncate partition (ds, hr='12');
TRUNCATE TABLE srcpart_truncate partition (ds, hr='12');
select * from srcpart_truncate where hr='12';
select count(*) from srcpart_truncate where hr='12';

-- truncate partitioned table
explain TRUNCATE TABLE srcpart_truncate;
TRUNCATE TABLE srcpart_truncate;
select * from srcpart_truncate;
select count(*) from srcpart_truncate;
