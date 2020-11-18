-- Test computation of stats for timestamp column

create table foo_n10(x date, y timestamp) stored as orc;
insert into foo_n10 values('1999-01-01', '1999-01-01 00:00:01'), ('2018-01-01', '2018-01-01 23:23:59');

analyze table foo_n10 compute statistics for columns;

set hive.compute.query.using.stats=true;
set test.comment=All queries need to be just metadata fetch tasks
set test.comment

explain select min(y) from foo_n10;
explain select max(y) from foo_n10;
explain select count(y) from foo_n10;
explain select count(y), max(y), min(y) from foo_n10;

select count(y), max(y), min(y) from foo_n10;
