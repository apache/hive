-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;
set hive.fetch.task.conversion=none;

create external table ice_p (id int, p string) partitioned by spec (p)
stored by iceberg stored as parquet tblproperties ('format-version'='2');

insert into ice_p values (1,'a'),(2,'a'),(7,'b');

-- 1) partition-level statistics for the current snapshot
set hive.iceberg.stats.collect.partlevel=true;
analyze table ice_p compute statistics for columns;
describe formatted ice_p id;
explain select * from ice_p where p='a';

-- 2) a new snapshot, then table-level statistics for it
insert into ice_p values (9,'c');
set hive.iceberg.stats.collect.partlevel=false;
analyze table ice_p compute statistics for columns;
describe formatted ice_p id;
-- table-level statistics serve the pruned set
explain select * from ice_p where p='a';

-- 3) toggle back: the current snapshot's file is table-level shaped, so the partition-level
-- read falls back to the older partition-level file of the ancestor snapshot
set hive.iceberg.stats.collect.partlevel=true;
explain select * from ice_p where p='a';
describe formatted ice_p id;

-- 4) recompute at the current snapshot with the flag on
analyze table ice_p compute statistics for columns;
explain select * from ice_p where p='a';
describe formatted ice_p id;

drop table ice_p;