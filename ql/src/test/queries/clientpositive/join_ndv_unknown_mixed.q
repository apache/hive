--! qt:dataset:src

-- HIVE-29625: join denominator with mixed known/unknown key NDVs.
-- The binary column carries the unknown NDV sentinel (-1); the cast expression over the
-- string column carries a known NDV. The known side must drive the join cardinality
-- estimate (rows_big * rows_dim / known_ndv), not a cross-product fallback.

create table j_ndv_big_bin stored as orc as
select cast(key as binary) as k, value as v from src;

create table j_ndv_dim_str stored as orc as
select distinct key as k from src;

explain select b.v from j_ndv_big_bin b join j_ndv_dim_str d on b.k = cast(d.k as binary);
