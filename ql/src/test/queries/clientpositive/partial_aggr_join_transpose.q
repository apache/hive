-- SORT_QUERY_RESULTS
-- Partial group by pushed below an inner join (hive.optimize.partial.aggr.join.transpose).

create table t_fact (k int, q int, v int, p decimal(7,2));
create table t_dim (k int, g string, c string);
create table t_dim2 (k int, h string);

-- k = 4 has no match in t_dim; v and p are NULL in one row
insert into t_fact values
  (1, 1, 1, 1.10), (1, 2, 2, 2.20), (1, 1, 3, 3.30),
  (2, 2, 4, 4.40), (2, 1, 5, 5.50),
  (3, 2, 6, 6.60), (3, 1, null, null), (3, 2, 8, 8.80),
  (4, 1, 9, 9.90), (4, 2, 10, 10.10);
-- k = 3 is duplicated: the join fans out the partial results
insert into t_dim values (1, 'a', 'x'), (2, 'a', null), (3, 'b', 'x'), (3, 'c', 'y');
-- unique keys, joined on t_fact.q
insert into t_dim2 values (1, 'x'), (2, 'y');

set hive.optimize.partial.aggr.join.transpose=true;

-- pushed: every supported function, over int and decimal arguments
explain
select d.g, count(*), count(f.v), sum(f.v), min(f.v), max(f.v), sum(f.p), min(f.p), max(f.p)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

select d.g, count(*), count(f.v), sum(f.v), min(f.v), max(f.v), sum(f.p), min(f.p), max(f.p)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- the same results without the optimization
set hive.optimize.partial.aggr.join.transpose=false;

select d.g, count(*), count(f.v), sum(f.v), min(f.v), max(f.v), sum(f.p), min(f.p), max(f.p)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

set hive.optimize.partial.aggr.join.transpose=true;

-- the same results without vectorization
set hive.vectorized.execution.enabled=false;

select d.g, count(*), count(f.v), sum(f.v), min(f.v), max(f.v), sum(f.p), min(f.p), max(f.p)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

set hive.vectorized.execution.enabled=true;

-- the same results in pass-through mode: the pushed group by stops hash aggregation at once
set hive.optimize.partial.aggr.join.transpose.min.reduction=0.0;
set hive.groupby.mapaggr.checkinterval=0;
set hive.vectorized.groupby.maxentries=0;

select d.g, count(*), count(f.v), sum(f.v), min(f.v), max(f.v), sum(f.p), min(f.p), max(f.p)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

set hive.optimize.partial.aggr.join.transpose.min.reduction=0.8;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.vectorized.groupby.maxentries=1000000;

-- pushed: a key of the join input the group by is pushed to that is also its join key
explain
select f.k, d.g, sum(f.v)
from t_fact f
join t_dim d on f.k = d.k
group by f.k, d.g;

select f.k, d.g, sum(f.v)
from t_fact f
join t_dim d on f.k = d.k
group by f.k, d.g;

-- pushed: no argument has columns, so the group by goes to the larger input
explain
select d.g, count(*)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

select d.g, count(*)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- pushed: no aggregation
explain
select d.g, f.q
from t_fact f
join t_dim d on f.k = d.k
group by d.g, f.q;

select d.g, f.q
from t_fact f
join t_dim d on f.k = d.k
group by d.g, f.q;

-- pushed: the column of a residual join filter becomes a pushed key
explain
select d.g, d.k, count(*), sum(f.v)
from t_fact f
join t_dim d on f.k = d.k and f.q < d.k
group by d.g, d.k;

select d.g, d.k, count(*), sum(f.v)
from t_fact f
join t_dim d on f.k = d.k and f.q < d.k
group by d.g, d.k;

-- pushed below both joins of a chain: the group by pushed below the upper join moves below the lower one
explain
select d.g, d2.h, count(*), sum(f.v)
from t_fact f
join t_dim d on f.k = d.k
join t_dim2 d2 on f.q = d2.k
group by d.g, d2.h;

select d.g, d2.h, count(*), sum(f.v)
from t_fact f
join t_dim d on f.k = d.k
join t_dim2 d2 on f.q = d2.k
group by d.g, d2.h;

-- not pushed: an argument references both inputs
explain
select d.g, sum(f.v + d.k)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- not pushed: two aggregates, each has one-one argument but from different inputs
explain
select d.g, sum(f.v), max(d.k)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- not pushed: an unsupported function
explain
select d.g, collect_set(f.v)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- not pushed: an outer join
explain
select d.g, sum(f.v)
from t_fact f
left join t_dim d on f.k = d.k
group by d.g;

-- not pushed: a pushed key has too many distinct values for the input rows
set hive.optimize.partial.aggr.join.transpose.max.key.ndv.ratio=0.1;

explain
select d.g, sum(f.v)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

set hive.optimize.partial.aggr.join.transpose.max.key.ndv.ratio=0.5;

-- not pushed: a CASE whose condition is on t_dim and whose value is on t_fact
explain
select d.g, sum(case when d.c = 'x' then f.v end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- a merged decimal SUM keeps its type
create table t_ctas as
select d.g, sum(f.p) s
from t_fact f
join t_dim d on f.k = d.k
group by d.g;
describe t_ctas;

-- a merged decimal SUM merges in Decimal64 (merge evaluator modes keep the partial type)
explain vectorization detail
select d.g, sum(f.p)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- pushed: the reduce sink of the group by is merged into the join's (its keys are the join keys),
-- so the group by above the join finalizes the partial results in the join's reducer
set hive.auto.convert.join.noconditionaltask=false;
set hive.convert.join.bucket.mapjoin.tez=false;

explain
select f.k, count(*), count(f.v), sum(f.v), min(f.v), max(f.v), sum(f.p)
from t_fact f
join t_dim d on f.k = d.k
group by f.k;

select f.k, count(*), count(f.v), sum(f.v), min(f.v), max(f.v), sum(f.p)
from t_fact f
join t_dim d on f.k = d.k
group by f.k;

-- the same results without vectorization
set hive.vectorized.execution.enabled=false;

select f.k, count(*), count(f.v), sum(f.v), min(f.v), max(f.v), sum(f.p)
from t_fact f
join t_dim d on f.k = d.k
group by f.k;

set hive.vectorized.execution.enabled=true;

-- the same results without the optimization
set hive.optimize.partial.aggr.join.transpose=false;

select f.k, count(*), count(f.v), sum(f.v), min(f.v), max(f.v), sum(f.p)
from t_fact f
join t_dim d on f.k = d.k
group by f.k;

set hive.optimize.partial.aggr.join.transpose=true;
set hive.auto.convert.join.noconditionaltask=true;

-- a pushed group by's output is not bucketed: the join must not become a bucket map join
create table t_dim_b (k int, g string) clustered by (k) into 2 buckets stored as orc;

insert into t_dim_b select k, g from t_dim;

set hive.auto.convert.join=true;
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.auto.convert.join.noconditionaltask.size=100000000;
set hive.exec.reducers.bytes.per.reducer=200;

explain
select f.k, sum(f.v)
from t_fact f
join t_dim_b d on f.k = d.k
group by f.k;

select f.k, sum(f.v)
from t_fact f
join t_dim_b d on f.k = d.k
group by f.k;
