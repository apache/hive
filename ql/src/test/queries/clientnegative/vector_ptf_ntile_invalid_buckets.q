set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;

CREATE TABLE vector_ptf_ntile_invalid_buckets(k int, ord int, nb int) stored as orc;

-- Only the first row of each partition has an invalid number of buckets; the later rows hold 3
INSERT INTO vector_ptf_ntile_invalid_buckets values
(1, 1, 0), (1, 2, 3), (1, 3, 3),
(2, 1, NULL), (2, 2, 3), (2, 3, 3),
(3, 1, -3), (3, 2, 3), (3, 3, 3);

-- Every query below fails, as it does in row mode; keep going until the last one
set hive.cli.errors.ignore=true;

-- 0 buckets (row mode divides by zero)
select k, ord,
ntile(nb) over (partition by k order by ord) as nt
from vector_ptf_ntile_invalid_buckets where k = 1;

-- a NULL number of buckets (row mode throws a NullPointerException)
select k, ord,
ntile(nb) over (partition by k order by ord) as nt
from vector_ptf_ntile_invalid_buckets where k = 2;

set hive.cli.errors.ignore=false;

-- -3 buckets, no larger in magnitude than the partition (row mode loops until it runs out of
-- memory)
select k, ord,
ntile(nb) over (partition by k order by ord) as nt
from vector_ptf_ntile_invalid_buckets where k = 3;
