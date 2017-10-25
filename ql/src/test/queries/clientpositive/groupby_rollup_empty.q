set hive.vectorized.execution.enabled=false;

drop table if exists tx1;
drop table if exists tx2;
create table tx1 (a integer,b integer,c integer);

select sum(c)
from tx1
;

select  sum(c),
        grouping(b),
	'NULL,1' as expected
from    tx1
where	a<0
group by a,b grouping sets ((), b, a);

select  sum(c),
        grouping(b),
	'NULL,1' as expected
from    tx1
where	a<0
group by rollup (b);

-- non-empty table

insert into tx1 values (1,1,1);

select  sum(c),
        grouping(b),
	'NULL,1' as expected
from    tx1
where	a<0
group by rollup (b);

select  sum(c),
        grouping(b),
	'1,1 and 1,0' as expected
from    tx1
group by rollup (b);


set hive.vectorized.execution.enabled=true;
create table tx2 (a integer,b integer,c integer,d double,u string) stored as orc;

explain
select  sum(c),
        grouping(b),
	'NULL,1' as expected
from    tx2
where	a<0
group by a,b grouping sets ((), b, a);


select sum(c),'NULL' as expected
from tx2;

select  sum(c),
	max(u),
	'asd',
        grouping(b),
	'NULL,1' as expected
from    tx2
where	a<0
group by a,b,d grouping sets ((), b, a, d);

