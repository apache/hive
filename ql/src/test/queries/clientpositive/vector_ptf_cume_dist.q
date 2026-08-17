set hive.vectorized.testing.reducer.batch.size=2;

CREATE TABLE vector_ptf_cume_dist_int(name string, rowindex int, mynumber int) stored as orc;

INSERT INTO vector_ptf_cume_dist_int values
-- a partition
('first', 1, 1),
('first', 2, 2),
('first', 3, 2),
('first', 4, NULL),
('first', 5, 3),
('first', 6, 3),
('first', 7, 4),
('first', 8, NULL),
('first', 9, 4),
('first', 10, 4),
('first', 11, 5),
('first', 12, 5),
('first', 13, NULL),
('first', 14, 5),
('first', 15, 5),
('first', 16, 6),
('first', 17, 6),
('first', 18, 6),
('first', 19, NULL),
('first', 20, 6),
('first', 21, 6),
-- another partition
('second', 22, 1),
('second', 23, 2),
('second', 24, 2),
('second', 25, NULL),
('second', 26, 3),
('second', 27, 3),
('second', 28, 4),
('second', 29, NULL),
('second', 30, 4),
('second', 31, 4),
('second', 32, 5),
('second', 33, 5),
('second', 34, NULL),
('second', 35, 5),
('second', 36, 5),
('second', 37, 6),
('second', 38, 6),
('second', 39, 6),
('second', 40, NULL),
('second', 41, 6),
('second', 42, 6),
-- null partition
(NULL, 43, 7),
(NULL, 44, 7);

-- NON-VECTORIZED
set hive.vectorized.execution.ptf.enabled=false;

select name, rowindex, mynumber,
cume_dist() over (partition by name order by mynumber) as cud
from vector_ptf_cume_dist_int;

select name, rowindex, mynumber,
rank() over (partition by name order by mynumber) as r,
dense_rank() over (partition by name order by mynumber) as dr,
cume_dist() over (partition by name order by mynumber) as cud
from vector_ptf_cume_dist_int;

-- VECTORIZED
set hive.vectorized.execution.ptf.enabled=true;

explain vectorization detail select name, rowindex, mynumber,
cume_dist() over (partition by name order by mynumber) as cud
from vector_ptf_cume_dist_int;

select name, rowindex, mynumber,
cume_dist() over (partition by name order by mynumber) as cud
from vector_ptf_cume_dist_int;

explain vectorization detail select name, rowindex, mynumber,
rank() over (partition by name order by mynumber) as r,
dense_rank() over (partition by name order by mynumber) as dr,
cume_dist() over (partition by name order by mynumber) as cud
from vector_ptf_cume_dist_int;

select name, rowindex, mynumber,
rank() over (partition by name order by mynumber) as r,
dense_rank() over (partition by name order by mynumber) as dr,
cume_dist() over (partition by name order by mynumber) as cud
from vector_ptf_cume_dist_int;
