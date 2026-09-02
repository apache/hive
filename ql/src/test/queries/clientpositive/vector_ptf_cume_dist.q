set hive.vectorized.testing.reducer.batch.size=2;

DROP TABLE IF EXISTS vector_ptf_cume_dist_int;

CREATE TABLE vector_ptf_cume_dist_int(name string, rowindex int, mynumber int) stored as orc;

INSERT INTO vector_ptf_cume_dist_int values
-- first partition (12 rows): cume_dist peer-group values 0.25, 1/3, 0.5, 2/3, 5/6, 1.0
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
-- second partition (10 rows): cume_dist peer-group values 0.2, 0.4, 0.5, 0.7, 0.8, 1.0
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

select name, rowindex, mynumber,
rank() over (order by mynumber) as r,
dense_rank() over (order by mynumber) as dr,
cume_dist() over (order by mynumber) as cud
from vector_ptf_cume_dist_int;

select name, rowindex, mynumber,
rank() over (partition by name) as r,
dense_rank() over (partition by name) as dr,
cume_dist() over (partition by name) as cud
from vector_ptf_cume_dist_int;

select name, rowindex, mynumber,
rank() over () as r,
dense_rank() over () as dr,
cume_dist() over () as cud
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

explain vectorization detail select name, rowindex, mynumber,
rank() over (order by mynumber) as r,
dense_rank() over (order by mynumber) as dr,
cume_dist() over (order by mynumber) as cud
from vector_ptf_cume_dist_int;

select name, rowindex, mynumber,
rank() over (order by mynumber) as r,
dense_rank() over (order by mynumber) as dr,
cume_dist() over (order by mynumber) as cud
from vector_ptf_cume_dist_int;

explain vectorization detail select name, rowindex, mynumber,
rank() over (partition by name) as r,
dense_rank() over (partition by name) as dr,
cume_dist() over (partition by name) as cud
from vector_ptf_cume_dist_int;

select name, rowindex, mynumber,
rank() over (partition by name) as r,
dense_rank() over (partition by name) as dr,
cume_dist() over (partition by name) as cud
from vector_ptf_cume_dist_int;

explain vectorization detail select name, rowindex, mynumber,
rank() over () as r,
dense_rank() over () as dr,
cume_dist() over () as cud
from vector_ptf_cume_dist_int;

select name, rowindex, mynumber,
rank() over () as r,
dense_rank() over () as dr,
cume_dist() over () as cud
from vector_ptf_cume_dist_int;
