-- what a compaction leaves behind for a table that had no column statistics
set hive.explain.user=false;
-- a fetch task prints no statistics, and it is the statistics this is about
set hive.fetch.task.conversion=none;
set hive.stats.autogather=true;
-- the inserts must gather nothing, so that what the compaction gathers is what shows
set hive.stats.column.autogather=false;
set hive.iceberg.stats.collect.partlevel=true;

-- a major compaction of an unpartitioned table reads every row it holds, so what it measures
-- describes the whole of it
create external table ice_comp_unpart (id bigint, p string)
stored by iceberg stored as orc
tblproperties ('format-version'='2', 'compactor.threshold.target.size'='1500');

insert into ice_comp_unpart values (1, 'a');
insert into ice_comp_unpart values (2, 'a');
insert into ice_comp_unpart values (3, 'a');
insert into ice_comp_unpart values (7, 'b');

explain select id from ice_comp_unpart where id > 0;

alter table ice_comp_unpart COMPACT 'major' and wait;

explain select id from ice_comp_unpart where id > 0;

select min(id), max(id) from ice_comp_unpart;

drop table ice_comp_unpart;

-- a major compaction of one partition reads every row of that partition, and none of the others,
-- so it describes that partition alone
create external table ice_comp (id bigint, p string)
    partitioned by spec (p)
stored by iceberg stored as orc
tblproperties ('format-version'='2', 'compactor.threshold.target.size'='1500',
    -- a compaction runs long after the session that queued it, so the granularity it keeps
    -- statistics at is asked for the way the compactor takes any of its settings
    'compactor.hive.iceberg.stats.collect.partlevel'='true');

insert into ice_comp values (1, 'a');
insert into ice_comp values (2, 'a');
insert into ice_comp values (3, 'a');
insert into ice_comp values (4, 'a');
insert into ice_comp values (7, 'b');

explain select id from ice_comp where id > 0;

alter table ice_comp PARTITION (p='a') COMPACT 'major' and wait;

explain select id from ice_comp where id > 0;

select min(id), max(id) from ice_comp;

drop table ice_comp;
