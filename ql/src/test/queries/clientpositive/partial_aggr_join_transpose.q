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
-- k = 3 is duplicated: the join fans out the partial results; its rows disagree on the CASE condition c = 'x',
-- the condition of k = 2 is NULL
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

-- pushed: a key of the pushed input that is also its join key
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

-- not pushed: an outer join (the expansion gate is relaxed, the outer join estimate is larger)
set hive.optimize.partial.aggr.join.transpose.max.join.expansion=2.0;
explain
select d.g, sum(f.v)
from t_fact f
left join t_dim d on f.k = d.k
group by d.g;
set hive.optimize.partial.aggr.join.transpose.max.join.expansion=1.1;

-- not pushed: a pushed key has too many distinct values for the input rows
set hive.optimize.partial.aggr.join.transpose.max.key.ndv.ratio=0.1;

explain
select d.g, sum(f.v)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

set hive.optimize.partial.aggr.join.transpose.max.key.ndv.ratio=0.5;

-- CASE arguments split across the join: the condition is on t_dim, the value on t_fact. The value is aggregated
-- below the join, the group by over the join merges IF(condition, partial, else).

-- pushed: every supported function, ELSE NULL (also a missing ELSE) and ELSE 0 for a SUM;
-- the condition of no row of g = 'c' is true
explain
select d.g,
  count(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v else 0 end),
  min(case when d.c = 'x' then f.v end),
  max(case when d.c = 'x' then f.v else null end),
  sum(case when d.c = 'x' then f.p else 0 end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

select d.g,
  count(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v else 0 end),
  min(case when d.c = 'x' then f.v end),
  max(case when d.c = 'x' then f.v else null end),
  sum(case when d.c = 'x' then f.p else 0 end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- the same results without the optimization
set hive.optimize.partial.aggr.join.transpose=false;

select d.g,
  count(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v else 0 end),
  min(case when d.c = 'x' then f.v end),
  max(case when d.c = 'x' then f.v else null end),
  sum(case when d.c = 'x' then f.p else 0 end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

set hive.optimize.partial.aggr.join.transpose=true;

-- the same results without vectorization
set hive.vectorized.execution.enabled=false;

select d.g,
  count(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v else 0 end),
  min(case when d.c = 'x' then f.v end),
  max(case when d.c = 'x' then f.v else null end),
  sum(case when d.c = 'x' then f.p else 0 end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

set hive.vectorized.execution.enabled=true;

-- the same results in pass-through mode: the pushed group by stops hash aggregation at once
set hive.optimize.partial.aggr.join.transpose.min.reduction=0.0;
set hive.groupby.mapaggr.checkinterval=0;
set hive.vectorized.groupby.maxentries=0;

select d.g,
  count(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v else 0 end),
  min(case when d.c = 'x' then f.v end),
  max(case when d.c = 'x' then f.v else null end),
  sum(case when d.c = 'x' then f.p else 0 end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

set hive.optimize.partial.aggr.join.transpose.min.reduction=0.8;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.vectorized.groupby.maxentries=1000000;

-- pushed: the join key is a pushed key besides f.q, allowed by the column of the condition
explain
select f.q, sum(case when d.c = 'x' then f.v end)
from t_fact f
join t_dim d on f.k = d.k
group by f.q;

select f.q, sum(case when d.c = 'x' then f.v end)
from t_fact f
join t_dim d on f.k = d.k
group by f.q;

-- pushed: the CASE of a decimal(2,2) value and ELSE 0 is a decimal(3,2), CBO does not cast the value (it casts only
-- to another type name): the value is cast below the join
explain
select d.g, sum(case when d.c = 'x' then cast(f.v / 100 as decimal(2,2)) else 0 end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

select d.g, sum(case when d.c = 'x' then cast(f.v / 100 as decimal(2,2)) else 0 end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- the merge of IF(condition, partial, NULL) and IF(condition, partial, 0) is vectorized over a map join
-- (a merge join is not vectorized)
set hive.auto.convert.join=true;

explain vectorization detail
select d.g,
  count(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

select d.g,
  count(case when d.c = 'x' then f.v end),
  sum(case when d.c = 'x' then f.v end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

set hive.auto.convert.join=false;

-- not pushed: a SUM whose ELSE is neither NULL nor 0
explain
select d.g, sum(case when d.c = 'x' then f.v else 1 end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- not pushed: a COUNT whose ELSE counts
explain
select d.g, count(case when d.c = 'x' then f.v else 0 end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- not pushed: a MIN whose ELSE is 0
explain
select d.g, min(case when d.c = 'x' then f.v else 0 end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- not pushed: two WHENs
explain
select d.g, sum(case when d.c = 'x' then f.v when d.c = 'y' then f.p end)
from t_fact f
join t_dim d on f.k = d.k
group by d.g;

-- not pushed: a condition over both inputs
explain
select d.g, sum(case when f.v > d.k then f.v end)
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
