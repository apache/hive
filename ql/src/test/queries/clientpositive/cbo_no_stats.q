set hive.explain.user=false;

create table foo (x int) ;
insert into foo values(1),(2),(3),(4),(5);

create table foo2 (y int) ;
insert into foo2 values(1), (2);

create table foo3 (z int) ;
insert into foo3 values(10), (11), (13), (14);

-- WE RETRIEVE COL STATS, SMART JOIN REORDERING
set hive.stats.fetch.column.stats=true;
explain
select count(case when (x=1 or false) then 1 else 0 end )
from foo
join foo2
  on foo.x = foo2.y
join foo3
  on foo.x = foo3.z
where x > 0;

-- WE DO NOT RETRIEVE COL STATS, NOT SO SMART JOIN REORDERING BUT AT LEAST FOLDING
set hive.stats.fetch.column.stats=false;
explain
select count(case when (x=1 or false) then 1 else 0 end )
from foo
join foo2
  on foo.x = foo2.y
join foo3
  on foo.x = foo3.z
where x > 0;

-- CALCITE IS DISABLED, FOLDING DOES NOT HAPPEN
set hive.cbo.enable=false;
explain
select count(case when (x=1 or false) then 1 else 0 end )
from foo
join foo2
  on foo.x = foo2.y
join foo3
  on foo.x = foo3.z
where x > 0;

drop table foo;
drop table foo2;
drop table foo3;
