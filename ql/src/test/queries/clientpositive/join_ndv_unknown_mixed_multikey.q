--! qt:dataset:src

-- HIVE-29625: multi-key join denominator with mixed known/unknown key NDVs, exercising the
-- correlated multi-key branch (hive.stats.correlated.multi.key.joins is on by default).
-- Key k carries the unknown NDV sentinel (-1) on the binary side and a known NDV (~309)
-- through the cast expression; key j has known NDVs (~10) on both sides. The k attribute
-- must drive the denominator with its known-side NDV; the unknown side must not collapse
-- the estimate to a cross product.

create table j_multi_big stored as orc as
select cast(key as binary) as k, substr(value, 1, 5) as j from src;

create table j_multi_dim stored as orc as
select distinct key as k, substr(value, 1, 5) as j from src;

explain select b.j from j_multi_big b join j_multi_dim d
on b.k = cast(d.k as binary) and b.j = d.j;

-- outer join: routes the unmatched-rows estimate (distinctUnmatched) through
-- calculateUnmatchedRowsForOuter with the unknown-aware guards
explain select b.j from j_multi_big b left outer join j_multi_dim d
on b.k = cast(d.k as binary) and b.j = d.j;
