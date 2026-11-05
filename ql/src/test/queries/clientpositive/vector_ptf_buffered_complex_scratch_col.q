set hive.explain.user=false;
set hive.vectorized.testing.reducer.batch.size=2;

DROP TABLE IF EXISTS vector_ptf_complex_scratch;

CREATE TABLE vector_ptf_complex_scratch(name string, rowindex int, mynumber int) stored as orc;

INSERT INTO vector_ptf_complex_scratch values
('first', 1, 1),
('first', 2, 1),
('first', 3, 2),
('first', 4, 3),
('first', 5, NULL),
('second', 6, 10),
('second', 7, 20),
('second', 8, 20),
(NULL, 9, 7);

-- The PTF buffers each partition, while array(...) is computed by the Select after the PTF.
-- Its array<int> scratch column must not be buffered with the partition.
set hive.vectorized.execution.ptf.enabled=false;

select name, rowindex,
ntile(2) over (partition by name order by rowindex) as nt,
cume_dist() over (partition by name order by rowindex) as cd,
sum(mynumber) over (partition by name order by rowindex) as s,
array(rowindex, mynumber)[1] as arr
from vector_ptf_complex_scratch;

set hive.vectorized.execution.ptf.enabled=true;

explain vectorization detail select name, rowindex,
ntile(2) over (partition by name order by rowindex) as nt,
cume_dist() over (partition by name order by rowindex) as cd,
sum(mynumber) over (partition by name order by rowindex) as s,
array(rowindex, mynumber)[1] as arr
from vector_ptf_complex_scratch;

select name, rowindex,
ntile(2) over (partition by name order by rowindex) as nt,
cume_dist() over (partition by name order by rowindex) as cd,
sum(mynumber) over (partition by name order by rowindex) as s,
array(rowindex, mynumber)[1] as arr
from vector_ptf_complex_scratch;

-- same shape without ntile: cume_dist alone also buffers the partition
select name, rowindex,
cume_dist() over (partition by name order by rowindex) as cd,
array(rowindex, mynumber)[1] as arr
from vector_ptf_complex_scratch;
