--! qt:dataset:src

-- HIVE-29625: map-side GROUP BY hash-table sizing with unknown key NDVs.
-- The binary columns carry the unknown NDV sentinel (-1). With two unknown grouping keys,
-- checkMapSideAggregation sizes the hash table once with the GROUP BY fallback for an unknown
-- group count (rows / 2), not as (rows / 2) ^ keys, so the 5,000 estimated entries fit and the
-- map-side estimate assumes hash aggregation.

create table gby_ndv_unk_a stored as orc as
select cast(key as binary) as k1 from src order by key limit 100;

create table gby_ndv_unk_b stored as orc as
select cast(value as binary) as k2 from src order by value limit 100;

explain select a.k1, b.k2, count(*) from gby_ndv_unk_a a cross join gby_ndv_unk_b b group by a.k1, b.k2;
