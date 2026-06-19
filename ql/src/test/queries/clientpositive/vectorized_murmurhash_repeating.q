set hive.llap.io.enabled=false;
set hive.vectorized.execution.enabled=true;

create table csv_table(id string, id2 string) row format delimited fields terminated by ',' stored as textfile;
LOAD DATA LOCAL INPATH '../../data/files/2048.csv' OVERWRITE INTO TABLE csv_table;

CREATE TABLE source(id string, id2 string) stored as orc;
insert overwrite table source select id, id2 from csv_table;

-- Test MurmurHashStringColStringCol
explain vectorization detail
select sum(murmur_hash(id, id2)) from source;
select sum(murmur_hash(id, id2)) from source;

-- Test MurmurHashIntColIntCol
explain vectorization detail
select sum(murmur_hash(cast(id as BIGINT), cast(id2 as BIGINT))) from source;
select sum(murmur_hash(cast(id as BIGINT), cast(id2 as BIGINT))) from source;

-- Test MurmurHashStringColIntCol: string is repeating
explain vectorization detail
select sum(murmur_hash(id, cast(id2 as BIGINT))) from source;
select sum(murmur_hash(id, cast(id2 as BIGINT))) from source;

-- Test MurmurHashStringColIntCol: long is repeating
explain vectorization detail
select sum(murmur_hash(id2, cast(id as BIGINT))) from source;
select sum(murmur_hash(id2, cast(id as BIGINT))) from source;
