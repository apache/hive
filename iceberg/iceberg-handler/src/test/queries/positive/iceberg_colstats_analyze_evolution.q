-- what ANALYZE stores for a table whose partitioning changed under it
set hive.explain.user=false;
-- a fetch task prints no statistics, and it is the statistics this is about
set hive.fetch.task.conversion=none;
set hive.stats.autogather=true;
-- the inserts must leave the stored statistics alone for what happens to them to be visible
set hive.stats.column.autogather=false;
set hive.iceberg.stats.collect.partlevel=true;

create external table ice_evo (id bigint, p string)
stored by iceberg tblproperties ('format-version'='2');

-- these rows belong to the synthetic partition, the one an unpartitioned spec names
insert into ice_evo values (1, 'a'), (100, 'b');

alter table ice_evo set partition spec (p);

insert into ice_evo values (5, 'a');

-- a full table ANALYZE reads every partition of every spec the table holds
analyze table ice_evo compute statistics for columns;

explain select id from ice_evo where id > 0;

select min(id), max(id) from ice_evo;

-- a write reaching one partition leaves the others describing themselves
insert into ice_evo values (7, 'a');

explain select id from ice_evo where id > 0;

select min(id), max(id) from ice_evo;

-- naming that partition measures it again; the ones it never named are carried
analyze table ice_evo partition (p='a') compute statistics for columns;

explain select id from ice_evo where id > 0;

select min(id), max(id) from ice_evo;

drop table ice_evo;

-- a table described in full, then partitioned differently and written to: what the new spec's
-- partitions hold is not described until an ANALYZE reads them
create external table ice_evo2 (id bigint, p string)
    partitioned by spec (p)
stored by iceberg tblproperties ('format-version'='2');

insert into ice_evo2 values (1, 'a'), (7, 'b');
analyze table ice_evo2 compute statistics for columns;

explain select id from ice_evo2 where id > 0;

select min(id), max(id) from ice_evo2;

alter table ice_evo2 set partition spec (p, truncate(1, p));

insert into ice_evo2 values (9, 'a');

explain select id from ice_evo2 where id > 0;

select min(id), max(id) from ice_evo2;

-- what the user sees before reaching for a partition scoped ANALYZE: partitions of both specs
show partitions ice_evo2;

-- naming a partition measures the one the current spec writes today; the older spec's p=a keeps
-- the statistics it already had, which no write since could have changed
analyze table ice_evo2 partition (p='a') compute statistics for columns;

explain select id from ice_evo2 where id > 0;

select min(id), max(id) from ice_evo2;

analyze table ice_evo2 compute statistics for columns;

explain select id from ice_evo2 where id > 0;

select min(id), max(id) from ice_evo2;

drop table ice_evo2;
