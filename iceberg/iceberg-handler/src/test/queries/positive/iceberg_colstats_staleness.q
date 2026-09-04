-- what a write leaves of the column statistics stored before it, at each granularity
set hive.explain.user=false;
-- a fetch task prints no statistics, and it is the statistics this is about
set hive.fetch.task.conversion=none;
set hive.stats.autogather=true;
-- the writes below must leave the stored statistics alone for what happens to them to be visible
set hive.stats.column.autogather=false;

-- an unpartitioned table keeps one set for the whole of it, and a write leaves none of it standing
create external table ice_stale_unpart (id bigint, p string)
stored by iceberg tblproperties ('format-version'='2');

insert into ice_stale_unpart values (1, 'a'), (2, 'b');
analyze table ice_stale_unpart compute statistics for columns;

explain select id from ice_stale_unpart where id > 0;

insert into ice_stale_unpart values (3, 'c');

explain select id from ice_stale_unpart where id > 0;

drop table ice_stale_unpart;

-- a partitioned table asked for whole-table statistics keeps one set too, on the same terms
set hive.iceberg.stats.collect.partlevel=false;

create external table ice_stale_tbllevel (id bigint, p string)
    partitioned by spec (p)
stored by iceberg tblproperties ('format-version'='2');

insert into ice_stale_tbllevel values (1, 'a'), (7, 'b'), (3, 'c');
analyze table ice_stale_tbllevel compute statistics for columns;

explain select id from ice_stale_tbllevel where id > 0;

insert into ice_stale_tbllevel values (9, 'a');

explain select id from ice_stale_tbllevel where id > 0;

drop table ice_stale_tbllevel;

-- kept per partition, only the partitions a write reached stop describing themselves, and what
-- the rest still describe is a part of what the scan reads
set hive.iceberg.stats.collect.partlevel=true;

create external table ice_stale_partlevel (id bigint, p string)
    partitioned by spec (p)
stored by iceberg tblproperties ('format-version'='2');

insert into ice_stale_partlevel values (1, 'a'), (7, 'b'), (3, 'c'), (5, 'd');
analyze table ice_stale_partlevel compute statistics for columns;

explain select id from ice_stale_partlevel where id > 0;

insert into ice_stale_partlevel values (9, 'a'), (9, 'b');

explain select id from ice_stale_partlevel where id > 0;

drop table ice_stale_partlevel;

-- statistics describe the columns stored with them, so a column added after them is described by
-- none: a scan wanting only that column holds none of what it asked for rather than part of it
set hive.iceberg.stats.collect.partlevel=false;

create external table ice_stale_newcol (id bigint, v string)
stored by iceberg tblproperties ('format-version'='2');

insert into ice_stale_newcol values (1, 'a'), (2, 'b');
analyze table ice_stale_newcol compute statistics for columns;

alter table ice_stale_newcol add columns (extra int);

-- a column they describe
explain select id from ice_stale_newcol;
-- only the column they do not
explain select extra from ice_stale_newcol;
-- one of each, which is what leaves a scan holding part of what it asked for
explain select id, extra from ice_stale_newcol;

drop table ice_stale_newcol;
