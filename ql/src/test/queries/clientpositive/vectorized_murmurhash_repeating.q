set hive.llap.io.enabled=false;
set hive.vectorized.execution.enabled=true;

CREATE TABLE source(id string, id2 string) stored as orc;
LOAD DATA LOCAL INPATH '../../data/files/2048.orc' overwrite into table source;

-- 2048.orc has 2 columns (id, id2) and 2048 rows.
-- For the first 1024 rows, both id and id2 are not repeating.
-- For the last 1024 rows, id is repeating while id2 is not.

-- Test MurmurHashStringColStringCol
explain
select sum(murmur_hash(id, id2)) from source;
select sum(murmur_hash(id, id2)) from source;

-- Test MurmurHashIntColIntCol
explain
select sum(murmur_hash(cast(id as BIGINT), cast(id2 as BIGINT))) from source;
select sum(murmur_hash(cast(id as BIGINT), cast(id2 as BIGINT))) from source;

-- Test MurmurHashStringColIntCol: string is repeating
explain
select sum(murmur_hash(id, cast(id2 as BIGINT))) from source;
select sum(murmur_hash(id, cast(id2 as BIGINT))) from source;

-- Test MurmurHashStringColIntCol: long is repeating
explain
select sum(murmur_hash(id2, cast(id as BIGINT))) from source;
select sum(murmur_hash(id2, cast(id as BIGINT))) from source;
