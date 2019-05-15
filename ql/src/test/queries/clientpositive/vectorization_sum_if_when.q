set hive.vectorized.execution.enabled=false;
drop table if exists vectorization_sum_if_when_a;
drop table if exists vectorization_sum_if_when_b;
create table vectorization_sum_if_when_a (x int) stored as orc;
insert into table vectorization_sum_if_when_a values (0), (1), (0), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL);
create table vectorization_sum_if_when_b (x int) stored as orc;
insert into table vectorization_sum_if_when_b select least(t1.x + t2.x + t3.x + t4.x, 1) from vectorization_sum_if_when_a t1, vectorization_sum_if_when_a t2, vectorization_sum_if_when_a t3, vectorization_sum_if_when_a t4;
select count(*), x from vectorization_sum_if_when_b group by x;

select sum(IF(x is null, 1, 0)), count(1) from vectorization_sum_if_when_b;
select sum(IF(x=1, 1, 0)), count(1) from vectorization_sum_if_when_b;
select sum((case WHEN x = 1 THEN 1 else 0 end)) from vectorization_sum_if_when_b;
select sum((case WHEN x = 1 THEN 1 else 0 end)), sum((case WHEN x = 1 THEN 1 when x is null then 0 else 0 end)) from vectorization_sum_if_when_b;

set hive.vectorized.execution.enabled=true;
select sum(IF(x is null, 1, 0)), count(1) from vectorization_sum_if_when_b;
select sum(IF(x=1, 1, 0)), count(1) from vectorization_sum_if_when_b;
select sum((case WHEN x = 1 THEN 1 else 0 end)) from vectorization_sum_if_when_b;
select sum((case WHEN x = 1 THEN 1 else 0 end)), sum((case WHEN x = 1 THEN 1 when x is null then 0 else 0 end)) from vectorization_sum_if_when_b;
