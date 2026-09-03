set hive.vectorized.testing.reducer.batch.size=2;

DROP TABLE IF EXISTS vector_ptf_percent_rank_int;

CREATE TABLE vector_ptf_percent_rank_int(name string, rowindex int, mynumber int) stored as orc;

INSERT INTO vector_ptf_percent_rank_int values
('five', 1, 10),
('five', 2, 20),
('five', 3, 30),
('five', 4, 40),
('five', 5, 50),
('six', 1, 10),
('six', 2, 20),
('six', 3, 30),
('six', 4, 40),
('six', 5, 50),
('six', 6, 60),
-- single-row partition: percent_rank 0.0
('lonely', 99, 42),
-- two-row null partition 
(NULL, 1, 100),
(NULL, 2, 100);

-- NON-VECTORIZED
set hive.vectorized.execution.ptf.enabled=false;

select name, rowindex, mynumber,
percent_rank() over (partition by name order by mynumber) as pr
from vector_ptf_percent_rank_int;

select name, rowindex, mynumber,
rank() over (partition by name order by mynumber) as r,
dense_rank() over (partition by name order by mynumber) as dr,
percent_rank() over (partition by name order by mynumber) as pr
from vector_ptf_percent_rank_int;

select name, rowindex, mynumber,
rank() over (order by mynumber) as r,
dense_rank() over (order by mynumber) as dr,
percent_rank() over (order by mynumber) as pr
from vector_ptf_percent_rank_int;

select name, rowindex, mynumber,
rank() over (partition by name) as r,
dense_rank() over (partition by name) as dr,
percent_rank() over (partition by name) as pr
from vector_ptf_percent_rank_int;

select name, rowindex, mynumber,
rank() over () as r,
dense_rank() over () as dr,
percent_rank() over () as pr
from vector_ptf_percent_rank_int;

-- VECTORIZED
set hive.vectorized.execution.ptf.enabled=true;

explain vectorization detail select name, rowindex, mynumber,
percent_rank() over (partition by name order by mynumber) as pr
from vector_ptf_percent_rank_int;

select name, rowindex, mynumber,
percent_rank() over (partition by name order by mynumber) as pr
from vector_ptf_percent_rank_int;

explain vectorization detail select name, rowindex, mynumber,
rank() over (partition by name order by mynumber) as r,
dense_rank() over (partition by name order by mynumber) as dr,
percent_rank() over (partition by name order by mynumber) as pr
from vector_ptf_percent_rank_int;

select name, rowindex, mynumber,
rank() over (partition by name order by mynumber) as r,
dense_rank() over (partition by name order by mynumber) as dr,
percent_rank() over (partition by name order by mynumber) as pr
from vector_ptf_percent_rank_int;

explain vectorization detail select name, rowindex, mynumber,
rank() over (order by mynumber) as r,
dense_rank() over (order by mynumber) as dr,
percent_rank() over (order by mynumber) as pr
from vector_ptf_percent_rank_int;

select name, rowindex, mynumber,
rank() over (order by mynumber) as r,
dense_rank() over (order by mynumber) as dr,
percent_rank() over (order by mynumber) as pr
from vector_ptf_percent_rank_int;

explain vectorization detail select name, rowindex, mynumber,
rank() over (partition by name) as r,
dense_rank() over (partition by name) as dr,
percent_rank() over (partition by name) as pr
from vector_ptf_percent_rank_int;

select name, rowindex, mynumber,
rank() over (partition by name) as r,
dense_rank() over (partition by name) as dr,
percent_rank() over (partition by name) as pr
from vector_ptf_percent_rank_int;

explain vectorization detail select name, rowindex, mynumber,
rank() over () as r,
dense_rank() over () as dr,
percent_rank() over () as pr
from vector_ptf_percent_rank_int;

select name, rowindex, mynumber,
rank() over () as r,
dense_rank() over () as dr,
percent_rank() over () as pr
from vector_ptf_percent_rank_int;
