set hive.stats.autogather=false;

create external table test_iceberg_stats (strcol string, intcol integer) partitioned by (pcol int) stored by iceberg;

insert into table test_iceberg_stats values ('abc', 1, 1);
insert into table test_iceberg_stats values ('def', 2, 2);
insert into table test_iceberg_stats values ('ghi', 3, 3);

set hive.iceberg.stats.source=iceberg;
-- No column stats is written in puffin files yet.
explain analyze table test_iceberg_stats compute statistics for columns;

-- Column stats is written in puffin files.
analyze table test_iceberg_stats compute statistics for columns;
explain analyze table test_iceberg_stats compute statistics for columns;

set hive.iceberg.stats.source=metastore;
-- No column stats must be seen when accessing metastore.
explain analyze table test_iceberg_stats compute statistics for columns;
