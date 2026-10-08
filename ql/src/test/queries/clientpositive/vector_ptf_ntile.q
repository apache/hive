set hive.explain.user=false;
set hive.vectorized.testing.reducer.batch.size=2;

DROP TABLE IF EXISTS vector_ptf_ntile_int;

CREATE TABLE vector_ptf_ntile_int(name string, rowindex int, mynumber int) stored as orc;

INSERT INTO vector_ptf_ntile_int values
-- first partition (12 rows)
('first', 1, 1),
('first', 2, 1),
('first', 3, 1),
('first', 4, 2),
('first', 5, 3),
('first', 6, 3),
('first', 7, 4),
('first', 8, 4),
('first', 9, 5),
('first', 10, 5),
('first', 11, NULL),
('first', 12, NULL),
-- second partition (10 rows)
('second', 22, 10),
('second', 23, 10),
('second', 24, 20),
('second', 25, 20),
('second', 26, 30),
('second', 27, 40),
('second', 28, 40),
('second', 29, 50),
('second', 30, NULL),
('second', 31, NULL),
-- null partition (2 rows)
(NULL, 43, 7),
(NULL, 44, 7);

-- NON-VECTORIZED: baseline output the vectorized run must match
set hive.vectorized.execution.ptf.enabled=false;

select name, rowindex, mynumber,
ntile(4) over (partition by name order by mynumber) as nt
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
ntile(3) over (partition by name order by mynumber) as nt3,
ntile(10) over (partition by name order by mynumber) as nt10
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
ntile(5) over (order by mynumber) as nt
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
ntile(4) over (partition by name) as nt
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
ntile(4) over () as nt
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
rank() over (partition by name order by mynumber) as r,
cume_dist() over (partition by name order by mynumber) as cd,
ntile(3) over (partition by name order by mynumber) as nt,
sum(mynumber) over (partition by name order by mynumber) as s
from vector_ptf_ntile_int;

-- VECTORIZED: same queries with PTF vectorization on — results must be identical
set hive.vectorized.execution.ptf.enabled=true;

explain vectorization detail select name, rowindex, mynumber,
ntile(4) over (partition by name order by mynumber) as nt
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
ntile(4) over (partition by name order by mynumber) as nt
from vector_ptf_ntile_int;

explain vectorization detail select name, rowindex, mynumber,
ntile(3) over (partition by name order by mynumber) as nt3,
ntile(10) over (partition by name order by mynumber) as nt10
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
ntile(3) over (partition by name order by mynumber) as nt3,
ntile(10) over (partition by name order by mynumber) as nt10
from vector_ptf_ntile_int;

explain vectorization detail select name, rowindex, mynumber,
ntile(5) over (order by mynumber) as nt
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
ntile(5) over (order by mynumber) as nt
from vector_ptf_ntile_int;

explain vectorization detail select name, rowindex, mynumber,
ntile(4) over (partition by name) as nt
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
ntile(4) over (partition by name) as nt
from vector_ptf_ntile_int;

explain vectorization detail select name, rowindex, mynumber,
ntile(4) over () as nt
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
ntile(4) over () as nt
from vector_ptf_ntile_int;

-- ntile mixed with streaming (rank), group-aggregated streaming (cume_dist) and buffered (sum) evaluators
explain vectorization detail select name, rowindex, mynumber,
rank() over (partition by name order by mynumber) as r,
cume_dist() over (partition by name order by mynumber) as cd,
ntile(3) over (partition by name order by mynumber) as nt,
sum(mynumber) over (partition by name order by mynumber) as s
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
rank() over (partition by name order by mynumber) as r,
cume_dist() over (partition by name order by mynumber) as cd,
ntile(3) over (partition by name order by mynumber) as nt,
sum(mynumber) over (partition by name order by mynumber) as s
from vector_ptf_ntile_int;

-- NOT VECTORIZED: a non-constant number of buckets falls back to row mode, which takes the value
-- from the first row of each partition
explain vectorization detail select name, rowindex,
ntile(rowindex) over (partition by name order by rowindex) as nt
from vector_ptf_ntile_int;

select name, rowindex,
ntile(rowindex) over (partition by name order by rowindex) as nt
from vector_ptf_ntile_int;
