set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;
set hive.fetch.task.conversion=none;

-- Using cint and cbigint in test queries
create table small_alltypesorc1a as select * from alltypesorc where cint is not null and cbigint is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc2a as select * from alltypesorc where cint is null and cbigint is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc3a as select * from alltypesorc where cint is not null and cbigint is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc4a as select * from alltypesorc where cint is null and cbigint is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;

select * from small_alltypesorc1a;
select * from small_alltypesorc2a;
select * from small_alltypesorc3a;
select * from small_alltypesorc4a;

create table small_alltypesorc_a stored as orc as select * from 
(select * from (select * from small_alltypesorc1a) sq1
 union all
 select * from (select * from small_alltypesorc2a) sq2
 union all
 select * from (select * from small_alltypesorc3a) sq3
 union all
 select * from (select * from small_alltypesorc4a) sq4) q;

ANALYZE TABLE small_alltypesorc_a COMPUTE STATISTICS;
ANALYZE TABLE small_alltypesorc_a COMPUTE STATISTICS FOR COLUMNS;

select * from small_alltypesorc_a;

explain vectorization detail
select count(*), sum(t1.c_cbigint) from (select c.cbigint as c_cbigint
from small_alltypesorc_a c
left outer join small_alltypesorc_a cd
  on cd.cint = c.cint 
left outer join small_alltypesorc_a hd
  on hd.cbigint = c.cbigint
) t1;

-- SORT_QUERY_RESULTS

select count(*), sum(t1.c_cbigint) from (select c.cbigint as c_cbigint
from small_alltypesorc_a c
left outer join small_alltypesorc_a cd
  on cd.cint = c.cint 
left outer join small_alltypesorc_a hd
  on hd.cbigint = c.cbigint
) t1;
