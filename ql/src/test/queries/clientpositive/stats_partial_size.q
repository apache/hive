set hive.stats.fetch.column.stats=true;

create table sample_partitioned (x int) partitioned by (y int);
insert into sample_partitioned partition(y=1) values (1),(2);
create temporary table sample as select * from sample_partitioned;
analyze table sample compute statistics for columns;

explain select sample_partitioned.x from sample_partitioned, sample where sample.y = sample_partitioned.y;
