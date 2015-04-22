SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;

create table small_alltypesorc1b as select * from alltypesorc where cint is not null and ctinyint is not null limit 10;
create table small_alltypesorc2b as select * from alltypesorc where cint is null and ctinyint is not null limit 10;
create table small_alltypesorc3b as select * from alltypesorc where cint is not null and ctinyint is null limit 10;
create table small_alltypesorc4b as select * from alltypesorc where cint is null and ctinyint is null limit 10;

create table small_alltypesorc_b stored as orc as select * from 
(select * from (select * from small_alltypesorc1b) sq1
 union all
 select * from (select * from small_alltypesorc2b) sq2
 union all
 select * from (select * from small_alltypesorc3b) sq3
 union all
 select * from (select * from small_alltypesorc4b) sq4) q;

ANALYZE TABLE small_alltypesorc_b COMPUTE STATISTICS;
ANALYZE TABLE small_alltypesorc_b COMPUTE STATISTICS FOR COLUMNS;

explain
select * 
from small_alltypesorc_b c
left outer join small_alltypesorc_b cd
  on cd.cint = c.cint;

-- SORT_QUERY_RESULTS

select * 
from small_alltypesorc_b c
left outer join small_alltypesorc_b cd
  on cd.cint = c.cint;

explain
select c.ctinyint 
from small_alltypesorc_b c
left outer join small_alltypesorc_b hd
  on hd.ctinyint = c.ctinyint;

-- SORT_QUERY_RESULTS

select c.ctinyint 
from small_alltypesorc_b c
left outer join small_alltypesorc_b hd
  on hd.ctinyint = c.ctinyint;

explain
select count(*) from (select c.ctinyint 
from small_alltypesorc_b c
left outer join small_alltypesorc_b cd
  on cd.cint = c.cint 
left outer join small_alltypesorc_b hd
  on hd.ctinyint = c.ctinyint
) t1
;

-- SORT_QUERY_RESULTS

select count(*) from (select c.ctinyint
from small_alltypesorc_b c
left outer join small_alltypesorc_b cd
  on cd.cint = c.cint 
left outer join small_alltypesorc_b hd
  on hd.ctinyint = c.ctinyint
) t1;