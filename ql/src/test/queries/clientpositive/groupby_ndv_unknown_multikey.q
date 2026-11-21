--! qt:dataset:src

-- HIVE-29625: map-side GROUP BY hash-table sizing with unknown key NDVs.
-- The binary columns carry the unknown NDV sentinel (-1). With two unknown grouping keys,
-- checkMapSideAggregation must keep the decision it made when unknown was stored as 0
-- (estimated hash table empty, hash aggregation assumed), so the Group By estimates stay
-- unchanged instead of the hash-table estimate growing as (rows / 2) ^ keys.

create table gby_ndv_unk_a stored as orc as
select cast(key as binary) as k1 from src order by key limit 100;

create table gby_ndv_unk_b stored as orc as
select cast(value as binary) as k2 from src order by value limit 100;

explain select a.k1, b.k2, count(*) from gby_ndv_unk_a a cross join gby_ndv_unk_b b group by a.k1, b.k2;
