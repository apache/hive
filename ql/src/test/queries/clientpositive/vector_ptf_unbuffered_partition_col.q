--! qt:dataset:cbo_t1


-- Case 1: PARTITION BY and ORDER BY are identical (isPartitionOrderBy=false).
EXPLAIN VECTORIZATION DETAIL
select key, sum(c_float) over(partition by c_float order by c_float) as s
from cbo_t1 order by key, s;

select key, sum(c_float) over(partition by c_float order by c_float) as s
from cbo_t1 order by key, s;

-- Case 2: multiple partition columns with partial ORDER BY overlap; arg avoids partition-only cols.
EXPLAIN VECTORIZATION DETAIL
select key, sum(c_float) over(partition by c_float, key order by c_float, value) as s
from cbo_t1 order by key, s;

select key, sum(c_float) over(partition by c_float, key order by c_float, value) as s
from cbo_t1 order by key, s;

-- Case 3: partition-only column referenced directly as a column argument (row-mode fallback).
EXPLAIN VECTORIZATION DETAIL
select key, sum(c_float) over(partition by c_float order by key) as s
from cbo_t1 order by key, s;

select key, sum(c_float) over(partition by c_float order by key) as s
from cbo_t1 order by key, s;

-- Case 4: partition-only column referenced inside an expression argument (row-mode fallback).
EXPLAIN VECTORIZATION DETAIL
select key, sum(c_float + 1) over(partition by c_float order by key) as s
from cbo_t1 order by key, s;

select key, sum(c_float + 1) over(partition by c_float order by key) as s
from cbo_t1 order by key, s;

