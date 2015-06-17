SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;

create table small_alltypesorc1a as select * from alltypesorc where cint is not null and ctinyint is not null order by cint, ctinyint limit 5;
create table small_alltypesorc2a as select * from alltypesorc where cint is null and ctinyint is not null order by cint, ctinyint limit 5;
create table small_alltypesorc3a as select * from alltypesorc where cint is not null and ctinyint is null order by cint, ctinyint limit 5;
create table small_alltypesorc4a as select * from alltypesorc where cint is null and ctinyint is null order by cint, ctinyint limit 5;

create table small_alltypesorc_a stored as orc as select * from 
(select * from (select * from small_alltypesorc1a) sq1
 union all
 select * from (select * from small_alltypesorc2a) sq2
 union all
 select * from (select * from small_alltypesorc3a) sq3
 union all
 select * from (select * from small_alltypesorc4a) sq4) q
order by cint, ctinyint, cstring1;


ANALYZE TABLE small_alltypesorc_a COMPUTE STATISTICS;
ANALYZE TABLE small_alltypesorc_a COMPUTE STATISTICS FOR COLUMNS;

explain
select count(*) from (select c.cstring1 
from small_alltypesorc_a c
left outer join small_alltypesorc_a cd
  on cd.cint = c.cint 
left outer join small_alltypesorc_a hd
  on hd.cstring1 = c.cstring1
) t1
;

-- SORT_QUERY_RESULTS

select count(*) from (select c.cstring1
from small_alltypesorc_a c
left outer join small_alltypesorc_a cd
  on cd.cint = c.cint 
left outer join small_alltypesorc_a hd
  on hd.cstring1 = c.cstring1
) t1;

explain
select count(*) from (select c.cstring1 
from small_alltypesorc_a c
left outer join small_alltypesorc_a cd
  on cd.cstring2 = c.cstring2 
left outer join small_alltypesorc_a hd
  on hd.cstring1 = c.cstring1
) t1
;

-- SORT_QUERY_RESULTS

select count(*) from (select c.cstring1
from small_alltypesorc_a c
left outer join small_alltypesorc_a cd
  on cd.cstring2 = c.cstring2 
left outer join small_alltypesorc_a hd
  on hd.cstring1 = c.cstring1
) t1;

explain
select count(*) from (select c.cstring1 
from small_alltypesorc_a c
left outer join small_alltypesorc_a cd
  on cd.cstring2 = c.cstring2 and cd.cbigint = c.cbigint
left outer join small_alltypesorc_a hd
  on hd.cstring1 = c.cstring1 and hd.cint = c.cint
) t1
;

-- SORT_QUERY_RESULTS

select count(*) from (select c.cstring1
from small_alltypesorc_a c
left outer join small_alltypesorc_a cd
  on cd.cstring2 = c.cstring2 and cd.cbigint = c.cbigint
left outer join small_alltypesorc_a hd
  on hd.cstring1 = c.cstring1 and hd.cint = c.cint
) t1;
