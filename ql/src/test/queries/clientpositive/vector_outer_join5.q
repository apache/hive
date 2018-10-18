--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

create table sorted_mod_4 stored as orc
as select ctinyint, pmod(cint, 4) as cmodint from alltypesorc
where cint is not null and ctinyint is not null
order by ctinyint;

ANALYZE TABLE sorted_mod_4 COMPUTE STATISTICS;
ANALYZE TABLE sorted_mod_4 COMPUTE STATISTICS FOR COLUMNS;

create table small_table stored
as orc as select ctinyint, cbigint from alltypesorc limit 100;

ANALYZE TABLE small_table COMPUTE STATISTICS;
ANALYZE TABLE small_table COMPUTE STATISTICS FOR COLUMNS;

explain vectorization detail formatted
select count(*) from (select s.*, st.*
from sorted_mod_4 s
left outer join small_table st
on s.ctinyint = st.ctinyint
) t1;

select count(*) from (select s.*, st.*
from sorted_mod_4 s
left outer join small_table st
on s.ctinyint = st.ctinyint
) t1;

explain vectorization detail formatted
select count(*) from (select s.ctinyint, s.cmodint, sm.cbigint 
from sorted_mod_4 s
left outer join small_table sm
on s.ctinyint = sm.ctinyint and s.cmodint = 2
) t1;

select count(*) from (select s.ctinyint, s.cmodint, sm.cbigint 
from sorted_mod_4 s
left outer join small_table sm
on s.ctinyint = sm.ctinyint and s.cmodint = 2
) t1;

explain vectorization detail formatted
select count(*) from (select s.ctinyint, s.cmodint, sm.cbigint 
from sorted_mod_4 s
left outer join small_table sm
on s.ctinyint = sm.ctinyint and pmod(s.ctinyint, 4) = s.cmodint
) t1;

select count(*) from (select s.ctinyint, s.cmodint, sm.cbigint 
from sorted_mod_4 s
left outer join small_table sm
on s.ctinyint = sm.ctinyint and pmod(s.ctinyint, 4) = s.cmodint
) t1;

explain vectorization detail formatted
select count(*) from (select s.ctinyint, s.cmodint, sm.cbigint 
from sorted_mod_4 s
left outer join small_table sm
on s.ctinyint = sm.ctinyint and s.ctinyint < 100
) t1;

select count(*) from (select s.ctinyint, s.cmodint, sm.cbigint 
from sorted_mod_4 s
left outer join small_table sm
on s.ctinyint = sm.ctinyint and s.ctinyint < 100
) t1;

explain vectorization detail formatted
select count(*) from (select s.*, sm.*, s2.* 
from sorted_mod_4 s
left outer join small_table sm
  on pmod(sm.cbigint, 8) = s.cmodint 
left outer join sorted_mod_4 s2
  on s2.ctinyint = s.ctinyint
) t1;

select count(*) from (select s.*, sm.*, s2.* 
from sorted_mod_4 s
left outer join small_table sm
  on pmod(sm.cbigint, 8) = s.cmodint 
left outer join sorted_mod_4 s2
  on s2.ctinyint = s.ctinyint
) t1;


create table mod_8_mod_4 stored as orc
as select pmod(ctinyint, 8) as cmodtinyint, pmod(cint, 4) as cmodint from alltypesorc
where cint is not null and ctinyint is not null;

ANALYZE TABLE mod_8_mod_4 COMPUTE STATISTICS;
ANALYZE TABLE mod_8_mod_4 COMPUTE STATISTICS FOR COLUMNS;

create table small_table2 stored
as orc as select pmod(ctinyint, 16) as cmodtinyint, cbigint from alltypesorc limit 100;

ANALYZE TABLE small_table2 COMPUTE STATISTICS;
ANALYZE TABLE small_table2 COMPUTE STATISTICS FOR COLUMNS;

explain vectorization detail formatted
select count(*) from (select s.*, st.*
from mod_8_mod_4 s
left outer join small_table2 st
on s.cmodtinyint = st.cmodtinyint
) t1;

select count(*) from (select s.*, st.*
from mod_8_mod_4 s
left outer join small_table2 st
on s.cmodtinyint = st.cmodtinyint
) t1;

explain vectorization detail formatted
select count(*) from (select s.cmodtinyint, s.cmodint, sm.cbigint 
from mod_8_mod_4 s
left outer join small_table2 sm
on s.cmodtinyint = sm.cmodtinyint and s.cmodint = 2
) t1;

select count(*) from (select s.cmodtinyint, s.cmodint, sm.cbigint 
from mod_8_mod_4 s
left outer join small_table2 sm
on s.cmodtinyint = sm.cmodtinyint and s.cmodint = 2
) t1;

explain vectorization detail formatted
select count(*) from (select s.cmodtinyint, s.cmodint, sm.cbigint 
from mod_8_mod_4 s
left outer join small_table2 sm
on s.cmodtinyint = sm.cmodtinyint and pmod(s.cmodtinyint, 4) = s.cmodint
) t1;

select count(*) from (select s.cmodtinyint, s.cmodint, sm.cbigint 
from mod_8_mod_4 s
left outer join small_table2 sm
on s.cmodtinyint = sm.cmodtinyint and pmod(s.cmodtinyint, 4) = s.cmodint
) t1;

explain vectorization detail formatted
select count(*) from (select s.cmodtinyint, s.cmodint, sm.cbigint 
from mod_8_mod_4 s
left outer join small_table2 sm
on s.cmodtinyint = sm.cmodtinyint and s.cmodtinyint < 3
) t1;

select count(*) from (select s.cmodtinyint, s.cmodint, sm.cbigint 
from mod_8_mod_4 s
left outer join small_table2 sm
on s.cmodtinyint = sm.cmodtinyint and s.cmodtinyint < 3
) t1;

explain vectorization detail formatted
select count(*) from (select s.*, sm.*, s2.* 
from mod_8_mod_4 s
left outer join small_table2 sm
  on pmod(sm.cbigint, 8) = s.cmodint 
left outer join mod_8_mod_4 s2
  on s2.cmodtinyint = s.cmodtinyint
) t1;

select count(*) from (select s.*, sm.*, s2.* 
from mod_8_mod_4 s
left outer join small_table2 sm
  on pmod(sm.cbigint, 8) = s.cmodint 
left outer join mod_8_mod_4 s2
  on s2.cmodtinyint = s.cmodtinyint
) t1;