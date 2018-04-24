SET hive.vectorized.execution.enabled=false;
set hive.spark.dynamic.partition.pruning=true;

-- SORT_QUERY_RESULTS

-- This qfile tests whether we can combine DPP sinks within a BaseWork

create table part_table_1 (col int) partitioned by (part_col int);
create table part_table_2 (col int) partitioned by (part_col int);
create table part_table_3 (col int) partitioned by (part_col1 int, part_col2 int);
create table regular_table (col int);

insert into table regular_table values (1);

alter table part_table_1 add partition (part_col=1);
insert into table part_table_1 partition (part_col=1) values (1), (2), (3), (4);

alter table part_table_1 add partition (part_col=2);
insert into table part_table_1 partition (part_col=2) values (1), (2), (3), (4);

alter table part_table_1 add partition (part_col=3);
insert into table part_table_1 partition (part_col=3) values (1), (2), (3), (4);

alter table part_table_2 add partition (part_col=1);
insert into table part_table_2 partition (part_col=1) values (1), (2), (3), (4);

alter table part_table_2 add partition (part_col=2);
insert into table part_table_2 partition (part_col=2) values (1), (2), (3), (4);

alter table part_table_3 add partition (part_col1=1, part_col2=1);
insert into table part_table_3 partition (part_col1=1, part_col2=1) values (1), (2), (3), (4);

-- dpp sinks should be combined

explain
select * from regular_table, part_table_1, part_table_2
where regular_table.col = part_table_1.part_col and regular_table.col = part_table_2.part_col;

select * from regular_table, part_table_1, part_table_2
where regular_table.col = part_table_1.part_col and regular_table.col = part_table_2.part_col;

set hive.auto.convert.join=true;

-- regular_table and part_table_2 are small tables, so DPP sinks don't need to be combined
explain
select * from regular_table, part_table_1, part_table_2
where regular_table.col = part_table_1.part_col and regular_table.col = part_table_2.part_col;

select * from regular_table, part_table_1, part_table_2
where regular_table.col = part_table_1.part_col and regular_table.col = part_table_2.part_col;

-- only regular_table is small table and DPP sinks are combined

explain
select * from regular_table, part_table_3
where regular_table.col=part_table_3.part_col1 and regular_table.col=part_table_3.part_col2;

select * from regular_table, part_table_3
where regular_table.col=part_table_3.part_col1 and regular_table.col=part_table_3.part_col2;