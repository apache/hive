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

-- Non-constant number of buckets: taken from the first row of each partition, as in row mode.
-- Each partition of vector_ptf_ntile_buckets holds its number of buckets (nb) in its first row by
-- ord; the later rows hold NULL, 0, negative and other counts, which must not be read.
DROP TABLE IF EXISTS vector_ptf_ntile_buckets;

CREATE TABLE vector_ptf_ntile_buckets(k int, ord int, nb int) stored as orc;

INSERT INTO vector_ptf_ntile_buckets values
-- a single bucket
(1, 1, 1), (1, 2, NULL), (1, 3, 0), (1, 4, -3), (1, 5, 7),
-- fewer buckets than rows, with a remainder: 7 rows in 2 buckets
(3, 6, 2), (3, 7, 0), (3, 8, NULL), (3, 9, 100), (3, 10, -1), (3, 11, 2), (3, 12, 5),
-- fewer buckets than rows, without a remainder: 6 rows in 3 buckets
(2, 13, 3), (2, 14, NULL), (2, 15, 0), (2, 16, 1), (2, 17, -6), (2, 18, 4),
-- as many buckets as rows
(4, 19, 4), (4, 20, 0), (4, 21, NULL), (4, 22, 1),
-- more buckets than rows: the largest int
(10, 23, 2147483647), (10, 24, NULL), (10, 25, 0),
-- a negative number of buckets larger in magnitude than the partition: one row per bucket
(-4, 26, -4), (-4, 27, 0), (-4, 28, NULL),
-- a single row
(5, 29, -2);

set hive.vectorized.execution.ptf.enabled=false;

select k, ord, nb,
ntile(nb) over (partition by k order by ord) as nt
from vector_ptf_ntile_buckets;

select k,
ntile(k * 2) over (partition by k) as nt
from vector_ptf_ntile_buckets;

select k, ord,
ntile(k) over (partition by k order by ord) as nt
from vector_ptf_ntile_buckets;

select name, rowindex,
ntile(rowindex) over (partition by name order by rowindex) as nt
from vector_ptf_ntile_int;

select name, rowindex, mynumber,
ntile(mynumber % 8 + 1) over (partition by name order by mynumber desc nulls last, rowindex) as nt
from vector_ptf_ntile_int;

select k, ord, nb,
rank() over (partition by k order by ord) as r,
ntile(3) over (partition by k order by ord) as nt3,
ntile(nb) over (partition by k order by ord) as nt,
sum(nb) over (partition by k order by ord) as s
from vector_ptf_ntile_buckets;

set hive.vectorized.execution.ptf.enabled=true;

-- column argument
explain vectorization detail select k, ord, nb,
ntile(nb) over (partition by k order by ord) as nt
from vector_ptf_ntile_buckets;

select k, ord, nb,
ntile(nb) over (partition by k order by ord) as nt
from vector_ptf_ntile_buckets;

-- expression argument; with PARTITION BY only, a batch holds several rows of the partition
explain vectorization detail select k,
ntile(k * 2) over (partition by k) as nt
from vector_ptf_ntile_buckets;

select k,
ntile(k * 2) over (partition by k) as nt
from vector_ptf_ntile_buckets;

-- partition key argument, which is not buffered but restored when the partition is forwarded
explain vectorization detail select k, ord,
ntile(k) over (partition by k order by ord) as nt
from vector_ptf_ntile_buckets;

select k, ord,
ntile(k) over (partition by k order by ord) as nt
from vector_ptf_ntile_buckets;

-- order key argument
select name, rowindex,
ntile(rowindex) over (partition by name order by rowindex) as nt
from vector_ptf_ntile_int;

-- expression argument, read from the first row in ORDER BY order rather than insertion order
select name, rowindex, mynumber,
ntile(mynumber % 8 + 1) over (partition by name order by mynumber desc nulls last, rowindex) as nt
from vector_ptf_ntile_int;

-- constant and non-constant ntile mixed with streaming (rank) and buffered (sum) evaluators while
-- spilling every batch to disk
set hive.vectorized.ptf.max.memory.buffering.batch.count=1;

select k, ord, nb,
rank() over (partition by k order by ord) as r,
ntile(3) over (partition by k order by ord) as nt3,
ntile(nb) over (partition by k order by ord) as nt,
sum(nb) over (partition by k order by ord) as s
from vector_ptf_ntile_buckets;

set hive.vectorized.ptf.max.memory.buffering.batch.count=25;

-- NOT VECTORIZED: a non-positive constant number of buckets
explain vectorization select k,
ntile(0) over (partition by k order by ord) as nt
from vector_ptf_ntile_buckets;

-- NOT VECTORIZED: lead and lag in the number of buckets expression
explain vectorization select name, rowindex,
ntile(lag(rowindex, 1, 4)) over (partition by name order by rowindex) as nt
from vector_ptf_ntile_int;

-- Default reducer batch size: each partition arrives as a single batch spanning several buckets
set hive.vectorized.testing.reducer.batch.size=-1;

set hive.vectorized.execution.ptf.enabled=false;

select name, rowindex, mynumber,
ntile(10) over (partition by name) as nt
from vector_ptf_ntile_int;

set hive.vectorized.execution.ptf.enabled=true;

select name, rowindex, mynumber,
ntile(10) over (partition by name) as nt
from vector_ptf_ntile_int;

set hive.vectorized.execution.ptf.enabled=false;

select k,
ntile(k * 2) over (partition by k) as nt
from vector_ptf_ntile_buckets;

set hive.vectorized.execution.ptf.enabled=true;

select k,
ntile(k * 2) over (partition by k) as nt
from vector_ptf_ntile_buckets;
