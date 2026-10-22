-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;
set hive.fetch.task.conversion=none;
set hive.iceberg.stats.collect.partlevel=true;

-- a per partition gather also folds the table's own entries from the partitions it wrote
create external table ice_folded (id int, amount int, p string) partitioned by spec (p)
stored by iceberg stored as parquet tblproperties ('format-version'='2');

insert into ice_folded values (1, 10, 'a'), (5, 50, 'a'), (9, 90, 'b'), (3, 30, 'c');
analyze table ice_folded compute statistics for columns;

-- the whole table, answered from what was folded rather than by merging every partition
describe formatted ice_folded id;
explain select count(*) from ice_folded where id > 4;

-- one partition, answered by that partition alone
explain select count(*) from ice_folded where p = 'a' and id > 4;

-- a write to one partition leaves the others describing themselves, and the fold with them
insert into ice_folded values (7, 70, 'b');
analyze table ice_folded partition (p='b') compute statistics for columns;
describe formatted ice_folded id;
explain select count(*) from ice_folded where id > 4;

drop table ice_folded;

-- a transform names what a value maps to, so the fold must still answer for every bucket
create external table ice_folded_bucket (id int, p int) partitioned by spec (bucket(4, p))
stored by iceberg stored as parquet tblproperties ('format-version'='2');

insert into ice_folded_bucket values (10, 1), (20, 2), (30, 3);
analyze table ice_folded_bucket compute statistics for columns;
describe formatted ice_folded_bucket id;
explain select count(*) from ice_folded_bucket where id > 15;

drop table ice_folded_bucket;
