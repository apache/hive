set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled = true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

-- TODO: add more stuff here after HIVE-5918 is fixed, such as cbigint and constants
explain vectorization expression
select cint / 0, ctinyint / 0, cbigint / 0, cdouble / 0.0 from alltypesorc limit 100;
select cint / 0, ctinyint / 0, cbigint / 0, cdouble / 0.0 from alltypesorc limit 100;

-- There are no zeros in the table, but there is 988888, so use it as zero

-- TODO: add more stuff here after HIVE-5918 is fixed, such as cbigint and constants as numerators
explain vectorization expression
select (cbigint - 988888L) as s1, cdouble / (cbigint - 988888L) as s2, 1.2 / (cbigint - 988888L) 
from alltypesorc where cbigint > 0 and cbigint < 100000000 order by s1, s2 limit 100;
select (cbigint - 988888L) as s1, cdouble / (cbigint - 988888L) as s2, 1.2 / (cbigint - 988888L) 
from alltypesorc where cbigint > 0 and cbigint < 100000000 order by s1, s2 limit 100;

-- There are no zeros in the table, but there is -200.0, so use it as zero

explain vectorization expression
select (cdouble + 200.0) as s1, cbigint / (cdouble + 200.0) as s2, (cdouble + 200.0) / (cdouble + 200.0), cbigint / (cdouble + 200.0), 3 / (cdouble + 200.0), 1.2 / (cdouble + 200.0) 
from alltypesorc where cdouble >= -500 and cdouble < -199 order by s1, s2 limit 100;
select (cdouble + 200.0) as s1, cbigint / (cdouble + 200.0) as s2, (cdouble + 200.0) / (cdouble + 200.0), cbigint / (cdouble + 200.0), 3 / (cdouble + 200.0), 1.2 / (cdouble + 200.0) 
from alltypesorc where cdouble >= -500 and cdouble < -199 order by s1, s2 limit 100;

-- There are no zeros in the table, but there is 1018195815 in cbigint, 528534767 in cint, so using it to do a divide by zero. ctinyint has a zero so can be used directly

explain vectorization expression
select cint, cbigint, ctinyint, (cint / (cint - 528534767)) as c1, (cbigint / (cbigint - 1018195815)) as c2, (ctinyint / ctinyint) as c3, (cint % (cint - 528534767)) as c4, (cbigint % (cbigint - 1018195815)), (ctinyint % ctinyint) as c3
from alltypesorc where cint > 500000000 or cdouble > 1000000000 or ctinyint = 0 order by c1, c2 limit 100;

select cint, cbigint, ctinyint, (cint / (cint - 528534767)) as c1, (cbigint / (cbigint - 1018195815)) as c2, (ctinyint / ctinyint) as c3, (cint % (cint - 528534767)) as c4, (cbigint % (cbigint - 1018195815)), (ctinyint % ctinyint) as c3
from alltypesorc where cint > 500000000 or cdouble > 1000000000 or ctinyint = 0 order by c1, c2 limit 100;
