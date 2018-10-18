
create table foo_n9(x date, y timestamp) stored as orc;

insert into foo_n9 values('1999-01-01', '1999-01-01 00:00:01'), ('2018-01-01', '2018-01-01 23:23:59');

analyze table foo_n9 compute statistics for columns;

set hive.compute.query.using.stats=true;

set test.comment=All queries need to be just metadata fetch tasks

explain select min(x) from foo_n9; 
explain select max(x) from foo_n9; 
explain select count(x) from foo_n9; 

explain select count(x), max(x), min(x) from foo_n9; 

select count(x), max(x), min(x) from foo_n9; 
