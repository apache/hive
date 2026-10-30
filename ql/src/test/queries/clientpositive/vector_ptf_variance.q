--! qt:dataset:alltypesorc
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
-- Several batches per partition, so the evaluators work across batch boundaries.
set hive.vectorized.testing.reducer.batch.size=3;
-- SORT_QUERY_RESULTS

-- Vectorized PTF for the variance family:
-- variance, var_pop, var_samp, std, stddev, stddev_pop, stddev_samp

DROP TABLE IF EXISTS vector_ptf_variance_t;

CREATE TABLE vector_ptf_variance_t(id int, p string, k int, ti tinyint, si smallint, i int,
  bi bigint, f float, d double, dec_s decimal(10,2), dec_l decimal(38,18)) STORED AS ORC;

INSERT INTO vector_ptf_variance_t VALUES
-- 'a': mixed values, nulls and ties on k
(1, 'a', 1, 1, 10, 100, 1000, 1.5, 1.25, 1.10, 1.123456789012345678),
(2, 'a', 1, 2, 20, 200, 2000, 2.5, 2.25, 2.20, 2.2),
(3, 'a', 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(4, 'a', 3, 4, 40, 400, 4000, 4.5, 4.25, 4.40, 4.4),
(5, 'a', 3, 5, 50, 500, 5000, 5.5, 5.25, 5.50, 5.5),
(6, 'a', 5, -6, -60, -600, -6000, -6.5, -6.25, -6.60, -6.6),
(7, 'a', 8, 7, 70, 700, 7000, 7.5, 7.25, 7.70, 7.7),
(8, 'a', 9, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
-- 'b': single row, sample variants are null, population variants are 0
(9, 'b', 1, 3, 30, 300, 3000, 3.5, 3.25, 3.30, 3.3),
-- 'c': only nulls
(10, 'c', 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(11, 'c', 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
-- 'd': the same value repeated
(12, 'd', 1, 5, 5, 5, 5, 5.0, 5.0, 5.00, 5),
(13, 'd', 2, 5, 5, 5, 5, 5.0, 5.0, 5.00, 5),
(14, 'd', 3, 5, 5, 5, 5, 5.0, 5.0, 5.00, 5),
(15, 'd', 4, 5, 5, 5, 5, 5.0, 5.0, 5.00, 5),
-- 'e': leading nulls, then extreme values
(16, 'e', 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(17, 'e', 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(18, 'e', 3, 127, 32767, 2147483647, 9223372036854775807, 3.4E38, 1.0E150, 99999999.99,
  99999999999999999999.999999999999999999),
(19, 'e', 4, -128, -32768, -2147483648, -9223372036854775807, -3.4E38, -1.0E150, -99999999.99,
  -99999999999999999999.999999999999999999),
(20, 'e', 5, 0, 0, 0, 0, 0, 0, 0, 0),
-- null partition
(21, NULL, 1, 1, 1, 1, 1, 1.0, 1.0, 1.00, 1),
(22, NULL, 2, 3, 3, 3, 3, 3.0, 3.0, 3.00, 3);


-- NON-VECTORIZED: baseline output the vectorized run must match
set hive.vectorized.execution.ptf.enabled=false;

-- 1. All the function names with the default frame (RANGE UNBOUNDED PRECEDING AND CURRENT ROW,
--    ties on k) and all the input types over the whole partition
select id, p, k, d,
variance(d) over w1 as var, var_pop(d) over w1 as var_pop, var_samp(d) over w1 as var_samp,
std(d) over w1 as std, stddev(d) over w1 as sd, stddev_pop(d) over w1 as sd_pop,
stddev_samp(d) over w1 as sd_samp,
var_samp(ti) over w2 as ti, var_samp(si) over w2 as si, var_samp(i) over w2 as i,
var_samp(bi) over w2 as bi, var_samp(f) over w2 as f, var_samp(d) over w2 as d_part,
var_samp(dec_s) over w2 as dec_s, var_samp(dec_l) over w2 as dec_l
from vector_ptf_variance_t
window w1 as (partition by p order by k), w2 as (partition by p);

-- 2. Streaming: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
select id, p, k,
var_pop(ti) over w as ti, stddev_samp(si) over w as si, variance(i) over w as i,
stddev_pop(bi) over w as bi, var_samp(f) over w as f, stddev(d) over w as d,
var_samp(dec_s) over w as dec_s, stddev_samp(dec_l) over w as dec_l
from vector_ptf_variance_t
window w as (partition by p order by k, id rows between unbounded preceding and current row);

-- 3. Bounded frames
select id, p, k, i,
var_samp(i) over (partition by p order by k, id rows between 2 preceding and current row) as r2p,
stddev_pop(i) over (partition by p order by k, id rows between 1 preceding and unbounded following) as r1p,
var_pop(i) over (partition by p order by k range between 2 preceding and current row) as rg2p,
stddev_samp(i) over (partition by p order by k, id rows between current row and unbounded following) as rcf
from vector_ptf_variance_t;

-- 4. Streaming and non-streaming evaluators together with other functions in one PTF
select id, p, k, dec_s,
row_number() over (partition by p order by k, id) as rn,
stddev_samp(dec_s) over (partition by p order by k, id rows between unbounded preceding and current row) as sd_stream,
var_pop(dec_s) over (partition by p order by k, id) as var_range,
avg(dec_s) over (partition by p order by k, id) as av,
variance(dec_l) over (partition by p order by k, id rows between 2 preceding and current row) as var_rows
from vector_ptf_variance_t;

-- 5. Expression and constant (repeating) arguments, ORDER BY without PARTITION BY and an empty
--    OVER clause
select id, p, k,
stddev_pop(si + 1) over (order by id) as sd_expr,
var_samp(d * 2) over (order by id rows between unbounded preceding and current row) as var_expr,
stddev_samp(f) over () as sd_all,
var_pop(2) over (partition by p order by k, id rows between unbounded preceding and current row) as var_const_stream,
stddev_samp(1.5) over (partition by p) as sd_const,
variance(cast(null as int)) over (partition by p order by k, id rows between unbounded preceding and current row) as var_null_stream,
stddev_pop(cast(null as double)) over (partition by p order by k) as sd_null
from vector_ptf_variance_t;


-- VECTORIZED: same queries with PTF vectorization on, results must be identical
set hive.vectorized.execution.ptf.enabled=true;

explain vectorization expression
select id, p, k, d,
variance(d) over w1 as var, var_pop(d) over w1 as var_pop, var_samp(d) over w1 as var_samp,
std(d) over w1 as std, stddev(d) over w1 as sd, stddev_pop(d) over w1 as sd_pop,
stddev_samp(d) over w1 as sd_samp,
var_samp(ti) over w2 as ti, var_samp(si) over w2 as si, var_samp(i) over w2 as i,
var_samp(bi) over w2 as bi, var_samp(f) over w2 as f, var_samp(d) over w2 as d_part,
var_samp(dec_s) over w2 as dec_s, var_samp(dec_l) over w2 as dec_l
from vector_ptf_variance_t
window w1 as (partition by p order by k), w2 as (partition by p);

select id, p, k, d,
variance(d) over w1 as var, var_pop(d) over w1 as var_pop, var_samp(d) over w1 as var_samp,
std(d) over w1 as std, stddev(d) over w1 as sd, stddev_pop(d) over w1 as sd_pop,
stddev_samp(d) over w1 as sd_samp,
var_samp(ti) over w2 as ti, var_samp(si) over w2 as si, var_samp(i) over w2 as i,
var_samp(bi) over w2 as bi, var_samp(f) over w2 as f, var_samp(d) over w2 as d_part,
var_samp(dec_s) over w2 as dec_s, var_samp(dec_l) over w2 as dec_l
from vector_ptf_variance_t
window w1 as (partition by p order by k), w2 as (partition by p);

explain vectorization expression
select id, p, k,
var_pop(ti) over w as ti, stddev_samp(si) over w as si, variance(i) over w as i,
stddev_pop(bi) over w as bi, var_samp(f) over w as f, stddev(d) over w as d,
var_samp(dec_s) over w as dec_s, stddev_samp(dec_l) over w as dec_l
from vector_ptf_variance_t
window w as (partition by p order by k, id rows between unbounded preceding and current row);

select id, p, k,
var_pop(ti) over w as ti, stddev_samp(si) over w as si, variance(i) over w as i,
stddev_pop(bi) over w as bi, var_samp(f) over w as f, stddev(d) over w as d,
var_samp(dec_s) over w as dec_s, stddev_samp(dec_l) over w as dec_l
from vector_ptf_variance_t
window w as (partition by p order by k, id rows between unbounded preceding and current row);

select id, p, k, i,
var_samp(i) over (partition by p order by k, id rows between 2 preceding and current row) as r2p,
stddev_pop(i) over (partition by p order by k, id rows between 1 preceding and unbounded following) as r1p,
var_pop(i) over (partition by p order by k range between 2 preceding and current row) as rg2p,
stddev_samp(i) over (partition by p order by k, id rows between current row and unbounded following) as rcf
from vector_ptf_variance_t;

explain vectorization expression
select id, p, k, dec_s,
row_number() over (partition by p order by k, id) as rn,
stddev_samp(dec_s) over (partition by p order by k, id rows between unbounded preceding and current row) as sd_stream,
var_pop(dec_s) over (partition by p order by k, id) as var_range,
avg(dec_s) over (partition by p order by k, id) as av,
variance(dec_l) over (partition by p order by k, id rows between 2 preceding and current row) as var_rows
from vector_ptf_variance_t;

select id, p, k, dec_s,
row_number() over (partition by p order by k, id) as rn,
stddev_samp(dec_s) over (partition by p order by k, id rows between unbounded preceding and current row) as sd_stream,
var_pop(dec_s) over (partition by p order by k, id) as var_range,
avg(dec_s) over (partition by p order by k, id) as av,
variance(dec_l) over (partition by p order by k, id rows between 2 preceding and current row) as var_rows
from vector_ptf_variance_t;

explain vectorization expression
select id, p, k,
stddev_pop(si + 1) over (order by id) as sd_expr,
var_samp(d * 2) over (order by id rows between unbounded preceding and current row) as var_expr,
stddev_samp(f) over () as sd_all,
var_pop(2) over (partition by p order by k, id rows between unbounded preceding and current row) as var_const_stream,
stddev_samp(1.5) over (partition by p) as sd_const,
variance(cast(null as int)) over (partition by p order by k, id rows between unbounded preceding and current row) as var_null_stream,
stddev_pop(cast(null as double)) over (partition by p order by k) as sd_null
from vector_ptf_variance_t;

select id, p, k,
stddev_pop(si + 1) over (order by id) as sd_expr,
var_samp(d * 2) over (order by id rows between unbounded preceding and current row) as var_expr,
stddev_samp(f) over () as sd_all,
var_pop(2) over (partition by p order by k, id rows between unbounded preceding and current row) as var_const_stream,
stddev_samp(1.5) over (partition by p) as sd_const,
variance(cast(null as int)) over (partition by p order by k, id rows between unbounded preceding and current row) as var_null_stream,
stddev_pop(cast(null as double)) over (partition by p order by k) as sd_null
from vector_ptf_variance_t;


-- Without CBO the function names are not normalized (variance, std, stddev)
set hive.cbo.enable=false;
set hive.vectorized.execution.ptf.enabled=false;

select id, p, k, d,
variance(d) over w as var, var_pop(d) over w as var_pop, var_samp(d) over w as var_samp,
std(d) over w as std, stddev(d) over w as sd, stddev_pop(d) over w as sd_pop,
stddev_samp(d) over w as sd_samp
from vector_ptf_variance_t
window w as (partition by p order by k);

set hive.vectorized.execution.ptf.enabled=true;

explain vectorization expression
select id, p, k, d,
variance(d) over w as var, var_pop(d) over w as var_pop, var_samp(d) over w as var_samp,
std(d) over w as std, stddev(d) over w as sd, stddev_pop(d) over w as sd_pop,
stddev_samp(d) over w as sd_samp
from vector_ptf_variance_t
window w as (partition by p order by k);

select id, p, k, d,
variance(d) over w as var, var_pop(d) over w as var_pop, var_samp(d) over w as var_samp,
std(d) over w as std, stddev(d) over w as sd, stddev_pop(d) over w as sd_pop,
stddev_samp(d) over w as sd_samp
from vector_ptf_variance_t
window w as (partition by p order by k);

set hive.cbo.enable=true;


-- Larger partitions spilled to disk: compare the vectorized results with row mode row by row
set hive.vectorized.testing.reducer.batch.size=-1;
set hive.vectorized.ptf.max.memory.buffering.batch.count=2;

DROP TABLE IF EXISTS vector_ptf_variance_big;

CREATE TABLE vector_ptf_variance_big STORED AS ORC AS
SELECT row_number() over (order by cint, cbigint, ctinyint, csmallint, cfloat, cdouble, cstring1,
  cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2) as id,
  ctinyint % 3 as p, ctinyint, csmallint, cint, cbigint, cfloat, cdouble,
  cast(cdouble as decimal(12,2)) as cdec_s, cast(cbigint as decimal(38,18)) as cdec_l
FROM alltypesorc;

set hive.vectorized.execution.ptf.enabled=false;

DROP TABLE IF EXISTS vector_ptf_variance_big_row;

CREATE TABLE vector_ptf_variance_big_row STORED AS ORC AS
SELECT id,
  var_samp(ctinyint) over w_stream as ti_stream,
  stddev_pop(csmallint) over w_stream as si_stream,
  variance(cfloat) over w_stream as f_stream,
  stddev_samp(cdec_l) over w_stream as decl_stream,
  var_pop(cint) over w_range as i_range,
  stddev_samp(cdouble) over w_range as d_range,
  var_samp(cdec_s) over w_range as decs_range,
  stddev(cbigint) over w_part as bi_part,
  var_pop(cdouble) over w_part as d_part,
  stddev_samp(cdec_s) over w_part as decs_part,
  var_samp(cbigint) over w_rows as bi_rows,
  stddev_pop(cfloat) over w_rows as f_rows,
  variance(cdec_l) over w_rows as decl_rows
FROM vector_ptf_variance_big
WINDOW
  w_stream as (partition by p order by id rows between unbounded preceding and current row),
  w_range as (partition by p order by csmallint),
  w_part as (partition by p),
  w_rows as (partition by p order by id rows between 5 preceding and current row);

set hive.vectorized.execution.ptf.enabled=true;

explain vectorization expression
SELECT id,
  var_samp(ctinyint) over w_stream as ti_stream,
  stddev_pop(csmallint) over w_stream as si_stream,
  variance(cfloat) over w_stream as f_stream,
  stddev_samp(cdec_l) over w_stream as decl_stream,
  var_pop(cint) over w_range as i_range,
  stddev_samp(cdouble) over w_range as d_range,
  var_samp(cdec_s) over w_range as decs_range,
  stddev(cbigint) over w_part as bi_part,
  var_pop(cdouble) over w_part as d_part,
  stddev_samp(cdec_s) over w_part as decs_part,
  var_samp(cbigint) over w_rows as bi_rows,
  stddev_pop(cfloat) over w_rows as f_rows,
  variance(cdec_l) over w_rows as decl_rows
FROM vector_ptf_variance_big
WINDOW
  w_stream as (partition by p order by id rows between unbounded preceding and current row),
  w_range as (partition by p order by csmallint),
  w_part as (partition by p),
  w_rows as (partition by p order by id rows between 5 preceding and current row);

DROP TABLE IF EXISTS vector_ptf_variance_big_vec;

CREATE TABLE vector_ptf_variance_big_vec STORED AS ORC AS
SELECT id,
  var_samp(ctinyint) over w_stream as ti_stream,
  stddev_pop(csmallint) over w_stream as si_stream,
  variance(cfloat) over w_stream as f_stream,
  stddev_samp(cdec_l) over w_stream as decl_stream,
  var_pop(cint) over w_range as i_range,
  stddev_samp(cdouble) over w_range as d_range,
  var_samp(cdec_s) over w_range as decs_range,
  stddev(cbigint) over w_part as bi_part,
  var_pop(cdouble) over w_part as d_part,
  stddev_samp(cdec_s) over w_part as decs_part,
  var_samp(cbigint) over w_rows as bi_rows,
  stddev_pop(cfloat) over w_rows as f_rows,
  variance(cdec_l) over w_rows as decl_rows
FROM vector_ptf_variance_big
WINDOW
  w_stream as (partition by p order by id rows between unbounded preceding and current row),
  w_range as (partition by p order by csmallint),
  w_part as (partition by p),
  w_rows as (partition by p order by id rows between 5 preceding and current row);

-- Expected: 12288 rows, a non-null result count per column, and 0 mismatches
SELECT count(*) as total_rows,
  count(r.ti_stream) as nonnull_ti_stream,
  count(r.decs_range) as nonnull_decs_range,
  count(r.bi_rows) as nonnull_bi_rows,
  sum(if(r.ti_stream <=> v.ti_stream, 0, 1)) + sum(if(r.si_stream <=> v.si_stream, 0, 1))
  + sum(if(r.f_stream <=> v.f_stream, 0, 1)) + sum(if(r.decl_stream <=> v.decl_stream, 0, 1))
  + sum(if(r.i_range <=> v.i_range, 0, 1)) + sum(if(r.d_range <=> v.d_range, 0, 1))
  + sum(if(r.decs_range <=> v.decs_range, 0, 1)) + sum(if(r.bi_part <=> v.bi_part, 0, 1))
  + sum(if(r.d_part <=> v.d_part, 0, 1)) + sum(if(r.decs_part <=> v.decs_part, 0, 1))
  + sum(if(r.bi_rows <=> v.bi_rows, 0, 1)) + sum(if(r.f_rows <=> v.f_rows, 0, 1))
  + sum(if(r.decl_rows <=> v.decl_rows, 0, 1)) as mismatches
FROM vector_ptf_variance_big_row r
JOIN vector_ptf_variance_big_vec v ON r.id = v.id;

DROP TABLE vector_ptf_variance_t;
DROP TABLE vector_ptf_variance_big;
DROP TABLE vector_ptf_variance_big_row;
DROP TABLE vector_ptf_variance_big_vec;
