set hive.stats.column.autogather=false;
set hive.cli.print.header=true;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;

explain vectorization detail
select 'key1', 'value1' from alltypesorc tablesample (1 rows);

select 'key1', 'value1' from alltypesorc tablesample (1 rows);


create table decimal_2 (t decimal(18,9)) stored as orc;

explain vectorization detail
insert overwrite table decimal_2
  select cast('17.29' as decimal(4,2)) from alltypesorc tablesample (1 rows);

insert overwrite table decimal_2
  select cast('17.29' as decimal(4,2)) from alltypesorc tablesample (1 rows);

select count(*) from decimal_2;

drop table decimal_2;


-- Dummy tables HIVE-13190
explain vectorization detail
select count(1) from (select * from (Select 1 a) x order by x.a) y;

select count(1) from (select * from (Select 1 a) x order by x.a) y;

explain vectorization detail
create temporary table dual as select 1;

create temporary table dual as select 1;

select * from dual;
