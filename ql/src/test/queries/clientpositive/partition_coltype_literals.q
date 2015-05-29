drop table if exists partcoltypenum;
create table partcoltypenum (key int, value string) partitioned by (tint tinyint, sint smallint, bint bigint);

-- add partition
alter table partcoltypenum add partition(tint=100Y, sint=20000S, bint=300000000000L);

-- describe partition
describe formatted partcoltypenum partition (tint=100, sint=20000S, bint='300000000000');

-- change partition file format
alter table partcoltypenum partition(tint=100, sint=20000S, bint='300000000000') set fileformat rcfile;
describe formatted partcoltypenum partition (tint=100Y, sint=20000S, bint=300000000000L);

-- change partition clusterby, sortby and bucket
alter table partcoltypenum partition(tint='100', sint=20000, bint=300000000000L) clustered by (key) sorted by (key desc) into 4 buckets;
describe formatted partcoltypenum partition (tint=100Y, sint=20000S, bint=300000000000L);

-- rename partition
alter table partcoltypenum partition(tint=100, sint=20000, bint=300000000000) rename to partition (tint=110Y, sint=22000S, bint=330000000000L);
describe formatted partcoltypenum partition (tint=110Y, sint=22000, bint='330000000000');

-- insert partition
insert into partcoltypenum partition (tint=110Y, sint=22000S, bint=330000000000L) select key, value from src limit 10;
insert into partcoltypenum partition (tint=110, sint=22000, bint=330000000000) select key, value from src limit 20;

-- select partition
select count(1) from partcoltypenum where tint=110Y and sint=22000S and bint=330000000000L;
select count(1) from partcoltypenum where tint=110Y and sint=22000 and bint='330000000000';

-- analyze partition statistics and columns statistics
analyze table partcoltypenum partition (tint=110Y, sint=22000S, bint=330000000000L) compute statistics;
describe extended partcoltypenum partition (tint=110Y, sint=22000S, bint=330000000000L);

analyze table partcoltypenum partition (tint=110Y, sint=22000S, bint=330000000000L) compute statistics for columns;
describe formatted partcoltypenum.key partition (tint=110Y, sint=22000S, bint=330000000000L);
describe formatted partcoltypenum.value partition (tint=110Y, sint=22000S, bint=330000000000L);

-- change table column type for partition
alter table partcoltypenum change key key decimal(10,0);
alter table partcoltypenum partition (tint=110Y, sint=22000S, bint=330000000000L) change key key decimal(10,0);
describe formatted partcoltypenum partition (tint=110Y, sint=22000S, bint=330000000000L);

-- change partititon column type
alter table partcoltypenum partition column (tint decimal(3,0));
describe formatted partcoltypenum partition (tint=110BD, sint=22000S, bint=330000000000L);

-- show partition
show partitions partcoltypenum partition (tint=110BD, sint=22000S, bint=330000000000L);

-- drop partition
alter table partcoltypenum drop partition (tint=110BD, sint=22000S, bint=330000000000L);
show partitions partcoltypenum;

-- change partition file location
insert into partcoltypenum partition (tint=100BD, sint=20000S, bint=300000000000L) select key, value from src limit 10;
describe formatted partcoltypenum partition (tint=100BD, sint=20000S, bint=300000000000L);
alter table partcoltypenum partition(tint=100BD, sint=20000S, bint=300000000000L) set location "file:/test/test/tint=1/sint=2/bint=3";
describe formatted partcoltypenum partition (tint=100BD, sint=20000S, bint=300000000000L);

drop table partcoltypenum;

drop table if exists partcoltypeothers;
create table partcoltypeothers (key int, value string) partitioned by (decpart decimal(6,2), datepart date);

set hive.typecheck.on.insert=false;
insert into partcoltypeothers partition (decpart = 1000.01BD, datepart = date '2015-4-13') select key, value from src limit 10;
show partitions partcoltypeothers;

set hive.typecheck.on.insert=true;
alter table partcoltypeothers partition(decpart = '1000.01BD', datepart = date '2015-4-13') rename to partition (decpart = 1000.01BD, datepart = date '2015-4-13');
show partitions partcoltypeothers;

drop table partcoltypeothers;


