-- Parquet bloom filter pruning under vectorized LLAP execution, where the filters are served from the
-- LLAP metadata cache after the first read of a file.
set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;

DROP TABLE IF EXISTS llap_bloom_parquet PURGE;

CREATE EXTERNAL TABLE llap_bloom_parquet (id bigint, name string)
STORED BY ICEBERG STORED AS PARQUET
TBLPROPERTIES ('format-version'='2', 'write.parquet.bloom-filter-enabled.column.id'='true');

INSERT INTO llap_bloom_parquet VALUES
(2, 'two'), (4, 'four'), (6, 'six'), (8, 'eight'), (10, 'ten');

-- absent from the bloom filter but inside the min/max range, so only the bloom filter can prune it;
-- this first read fills the cache
SELECT count(*) FROM llap_bloom_parquet WHERE id = 5;

-- under cache.only the reader may not fall back to the file, so this answers only if the bloom filter
-- itself came from the cache
set hive.llap.io.cache.only=true;
SELECT count(*) FROM llap_bloom_parquet WHERE id = 5;
set hive.llap.io.cache.only=false;

-- a value the bloom filter does contain must survive pruning; count(*) keeps this off the fetch-task
-- path, which runs in the client JVM and would never reach the LLAP reader
SELECT count(*) FROM llap_bloom_parquet WHERE id = 6;

DROP TABLE llap_bloom_parquet PURGE;

-- A file of several row groups, where statistics leave a different row group standing per predicate. Each
-- filter is cached under its own offset, so serving one never depends on which query cached it.
DROP TABLE IF EXISTS llap_bloom_multi PURGE;

CREATE EXTERNAL TABLE llap_bloom_multi (id bigint, name string)
STORED BY ICEBERG STORED AS PARQUET
TBLPROPERTIES ('format-version'='2', 'write.parquet.bloom-filter-enabled.column.id'='true',
  'write.parquet.bloom-filter-max-bytes'='1024', 'write.parquet.row-group-size-bytes'='1024');

INSERT INTO llap_bloom_multi
SELECT pos * 2, concat('n', pos) FROM (SELECT 1) x LATERAL VIEW posexplode(split(space(399), ' ')) e AS pos, val;

-- odd ids are absent everywhere, and each lands in a different row group
SELECT count(*) FROM llap_bloom_multi WHERE id = 51;
SELECT count(*) FROM llap_bloom_multi WHERE id = 651;

set hive.llap.io.cache.only=true;
SELECT count(*) FROM llap_bloom_multi WHERE id = 51;
SELECT count(*) FROM llap_bloom_multi WHERE id = 651;
set hive.llap.io.cache.only=false;

-- even ids are present, and must survive pruning against filters served from the cache
SELECT count(*) FROM llap_bloom_multi WHERE id = 4;
SELECT count(*) FROM llap_bloom_multi WHERE id = 700;

DROP TABLE llap_bloom_multi PURGE;
