-- HIVE-29516: NPE in StatsUtils.updateStats when removing semijoin by benefit and column statistics are missing
set hive.stats.fetch.column.stats=false;

create table big (id int, val string) partitioned by (bday int);
alter table big add partition (bday=20260410);
alter table big partition (bday=20260410) update statistics set ('numRows' = '1000000000');

create table small (id int, val string);
alter table small update statistics set ('numRows' = '1000');

explain select big.val, small.val from big join small on big.id = small.id;
