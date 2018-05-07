set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.fetch.task.conversion=none;

create table test_vector_bround(v0 double, v1 double) stored as orc;
insert into table test_vector_bround
values
(2.5, 1.25),
(3.5, 1.35),
(-2.5, -1.25),
(-3.5, -1.35),
(2.49, 1.249),
(3.49, 1.349),
(2.51, 1.251),
(3.51, 1.351);

-- Add a single NULL row that will come from ORC as isRepeated.
insert into test_vector_bround values (NULL, NULL);

set hive.vectorized.execution.enabled=true;

explain vectorization detail
select bround(v0), bround(v1, 1) from test_vector_bround;
select bround(v0), bround(v1, 1) from test_vector_bround;
