--! qt:dataset:alltypesorc
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;
set hive.fetch.task.conversion=none;

-- Using cint and cstring1 in test queries
create table small_alltypesorc1a_n1 as select * from alltypesorc where cint is not null and cstring1 is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc2a_n1 as select * from alltypesorc where cint is null and cstring1 is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc3a_n1 as select * from alltypesorc where cint is not null and cstring1 is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc4a_n1 as select * from alltypesorc where cint is null and cstring1 is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;

select * from small_alltypesorc1a_n1;
select * from small_alltypesorc2a_n1;
select * from small_alltypesorc3a_n1;
select * from small_alltypesorc4a_n1;

create table small_alltypesorc_a_n1 stored as orc as select * from 
(select * from (select * from small_alltypesorc1a_n1) sq1
 union all
 select * from (select * from small_alltypesorc2a_n1) sq2
 union all
 select * from (select * from small_alltypesorc3a_n1) sq3
 union all
 select * from (select * from small_alltypesorc4a_n1) sq4) q;

ANALYZE TABLE small_alltypesorc_a_n1 COMPUTE STATISTICS;
ANALYZE TABLE small_alltypesorc_a_n1 COMPUTE STATISTICS FOR COLUMNS;

select * from small_alltypesorc_a_n1;
explain vectorization detail formatted
select count(*) from (select c.cstring1 
from small_alltypesorc_a_n1 c
left outer join small_alltypesorc_a_n1 cd
  on cd.cint = c.cint 
left outer join small_alltypesorc_a_n1 hd
  on hd.cstring1 = c.cstring1
) t1
;

-- SORT_QUERY_RESULTS

select count(*) from (select c.cstring1
from small_alltypesorc_a_n1 c
left outer join small_alltypesorc_a_n1 cd
  on cd.cint = c.cint 
left outer join small_alltypesorc_a_n1 hd
  on hd.cstring1 = c.cstring1
) t1;

explain vectorization detail formatted
select count(*) from (select c.cstring1 
from small_alltypesorc_a_n1 c
left outer join small_alltypesorc_a_n1 cd
  on cd.cstring2 = c.cstring2 
left outer join small_alltypesorc_a_n1 hd
  on hd.cstring1 = c.cstring1
) t1
;

-- SORT_QUERY_RESULTS

select count(*) from (select c.cstring1
from small_alltypesorc_a_n1 c
left outer join small_alltypesorc_a_n1 cd
  on cd.cstring2 = c.cstring2 
left outer join small_alltypesorc_a_n1 hd
  on hd.cstring1 = c.cstring1
) t1;

explain vectorization detail formatted
select count(*) from (select c.cstring1 
from small_alltypesorc_a_n1 c
left outer join small_alltypesorc_a_n1 cd
  on cd.cstring2 = c.cstring2 and cd.cbigint = c.cbigint
left outer join small_alltypesorc_a_n1 hd
  on hd.cstring1 = c.cstring1 and hd.cint = c.cint
) t1
;

-- SORT_QUERY_RESULTS

select count(*) from (select c.cstring1
from small_alltypesorc_a_n1 c
left outer join small_alltypesorc_a_n1 cd
  on cd.cstring2 = c.cstring2 and cd.cbigint = c.cbigint
left outer join small_alltypesorc_a_n1 hd
  on hd.cstring1 = c.cstring1 and hd.cint = c.cint
) t1;
