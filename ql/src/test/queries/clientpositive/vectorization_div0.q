--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled = true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

-- TODO: add more stuff here after HIVE-5918 is fixed, such as cbigint and constants
explain vectorization expression
select cint, cint / 0 as cint_div, ctinyint, ctinyint / 0 as ctinyint_div, cbigint, cbigint / 0 as cbigint_div, cdouble, cdouble / 0.0 as cdouble_div
from alltypesorc order by cint, ctinyint, cbigint, cdouble limit 100;

select cint, cint / 0 as cint_div, ctinyint, ctinyint / 0 as ctinyint_div, cbigint, cbigint / 0 as cbigint_div, cdouble, cdouble / 0.0 as cdouble_div
from alltypesorc order by cint, ctinyint, cbigint, cdouble limit 100;

-- There are no zeros in the table, but there is 988888, so use it as zero

-- TODO: add more stuff here after HIVE-5918 is fixed, such as cbigint and constants as numerators
explain vectorization expression
select (cbigint - 988888L) as s1, cdouble / (cbigint - 988888L) as s2, 1.2 / (cbigint - 988888L) as s3 
from alltypesorc where cbigint > 0 and cbigint < 100000000 order by s1, s2, s3 limit 100;

select (cbigint - 988888L) as s1, cdouble / (cbigint - 988888L) as s2, 1.2 / (cbigint - 988888L) as s3 
from alltypesorc where cbigint > 0 and cbigint < 100000000 order by s1, s2, s3 limit 100;

-- There are no zeros in the table, but there is -200.0, so use it as zero

explain vectorization expression
select (cdouble + 200.0) as s1, cbigint / (cdouble + 200.0) as s2, (cdouble + 200.0) / (cdouble + 200.0) as s3, cbigint / (cdouble + 200.0) as s4, 3 / (cdouble + 200.0) as s5, 1.2 / (cdouble + 200.0) as s6
from alltypesorc where cdouble >= -500 and cdouble < -199 order by s1, s2, s3, s4, s5, s6 limit 100;

select (cdouble + 200.0) as s1, cbigint / (cdouble + 200.0) as s2, (cdouble + 200.0) / (cdouble + 200.0) as s3, cbigint / (cdouble + 200.0) as s4, 3 / (cdouble + 200.0) as s5, 1.2 / (cdouble + 200.0) as s6
from alltypesorc where cdouble >= -500 and cdouble < -199 order by s1, s2, s3, s4, s5, s6 limit 100;

-- There are no zeros in the table, but there is 1018195815 in cbigint, 528534767 in cint, so using it to do a divide by zero. ctinyint has a zero so can be used directly

explain vectorization expression
select cint, cbigint, ctinyint, (cint / (cint - 528534767)) as c1, (cbigint / (cbigint - 1018195815)) as c2, (ctinyint / ctinyint) as c3, (cint % (cint - 528534767)) as c4, (cbigint % (cbigint - 1018195815)) as c5, (ctinyint % ctinyint) as c6
from alltypesorc where cint > 500000000 or cdouble > 1000000000 or ctinyint = 0 order by cint, cbigint, ctinyint, c1, c2, c3, c4, c5, c6 limit 100;

select cint, cbigint, ctinyint, (cint / (cint - 528534767)) as c1, (cbigint / (cbigint - 1018195815)) as c2, (ctinyint / ctinyint) as c3, (cint % (cint - 528534767)) as c4, (cbigint % (cbigint - 1018195815)) as c5, (ctinyint % ctinyint) as c6
from alltypesorc where cint > 500000000 or cdouble > 1000000000 or ctinyint = 0 order by cint, cbigint, ctinyint, c1, c2, c3, c4, c5, c6 limit 100;
