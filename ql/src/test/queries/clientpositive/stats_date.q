
create table foo(x date, y timestamp) stored as orc;

insert into foo values('1999-01-01', '1999-01-01 00:00:01'), ('2018-01-01', '2018-01-01 23:23:59');

analyze table foo compute statistics for columns;

set hive.compute.query.using.stats=true;

set test.comment=All queries need to be just metadata fetch tasks

explain select min(x) from foo; 
explain select max(x) from foo; 
explain select count(x) from foo; 

explain select count(x), max(x), min(x) from foo; 

select count(x), max(x), min(x) from foo; 
