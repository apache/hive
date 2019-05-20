set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.mapjoin.native.enabled=false;
set hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled=false;

set hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
set hive.stats.fetch.column.stats=false;

------------------------------------------------------------------------------------------
-- FULL OUTER Vectorized PASS-TRUE Mode MapJoin variation for OPTIMIZED hash table implementation.
------------------------------------------------------------------------------------------

-- SORT_QUERY_RESULTS

------------------------------------------------------------------------------------------
-- DYNAMIC PARTITION HASH JOIN
------------------------------------------------------------------------------------------

set hive.optimize.dynamic.partition.hashjoin=true;

set hive.mapjoin.hybridgrace.hashtable=false;

-- NOTE: Use very small sizes here to skip SHARED MEMORY MapJoin and force usage
-- NOTE: of DYNAMIC PARTITION HASH JOIN instead.
set hive.auto.convert.join.noconditionaltask.size=500;
set hive.exec.reducers.bytes.per.reducer=500;

------------------------------------------------------------------------------------------
-- Single LONG key
------------------------------------------------------------------------------------------

CREATE TABLE fullouter_long_big_1a_txt(key bigint)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_long_big_1a.txt' OVERWRITE INTO TABLE fullouter_long_big_1a_txt;
CREATE TABLE fullouter_long_big_1a STORED AS ORC AS SELECT * FROM fullouter_long_big_1a_txt;

CREATE TABLE fullouter_long_big_1a_nonull_txt(key bigint)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_long_big_1a_nonull.txt' OVERWRITE INTO TABLE fullouter_long_big_1a_nonull_txt;
CREATE TABLE fullouter_long_big_1a_nonull STORED AS ORC AS SELECT * FROM fullouter_long_big_1a_nonull_txt;

CREATE TABLE fullouter_long_small_1a_txt(key bigint, s_date date)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_long_small_1a.txt' OVERWRITE INTO TABLE fullouter_long_small_1a_txt;
CREATE TABLE fullouter_long_small_1a STORED AS ORC AS SELECT * FROM fullouter_long_small_1a_txt;

CREATE TABLE fullouter_long_small_1a_nonull_txt(key bigint, s_date date)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_long_small_1a_nonull.txt' OVERWRITE INTO TABLE fullouter_long_small_1a_nonull_txt;
CREATE TABLE fullouter_long_small_1a_nonull STORED AS ORC AS SELECT * FROM fullouter_long_small_1a_nonull_txt;

analyze table fullouter_long_big_1a compute statistics;
analyze table fullouter_long_big_1a compute statistics for columns;
analyze table fullouter_long_big_1a_nonull compute statistics;
analyze table fullouter_long_big_1a_nonull compute statistics for columns;
analyze table fullouter_long_small_1a compute statistics;
analyze table fullouter_long_small_1a compute statistics for columns;
analyze table fullouter_long_small_1a_nonull compute statistics;
analyze table fullouter_long_small_1a_nonull compute statistics for columns;

-- Do first one with FULL OUTER MapJoin NOT Enabled.
SET hive.mapjoin.full.outer=false;
EXPLAIN VECTORIZATION DETAIL
SELECT b.key, s.key, s.s_date FROM fullouter_long_big_1a b FULL OUTER JOIN fullouter_long_small_1a s ON b.key = s.key
order by b.key;

SELECT b.key, s.key, s.s_date FROM fullouter_long_big_1a b FULL OUTER JOIN fullouter_long_small_1a s ON b.key = s.key
order by b.key;

SET hive.mapjoin.full.outer=true;

EXPLAIN VECTORIZATION DETAIL
SELECT b.key, s.key, s.s_date FROM fullouter_long_big_1a b FULL OUTER JOIN fullouter_long_small_1a s ON b.key = s.key
order by b.key;

SELECT b.key, s.key, s.s_date FROM fullouter_long_big_1a b FULL OUTER JOIN fullouter_long_small_1a s ON b.key = s.key
order by b.key;

-- Big table without NULL key(s).
SELECT b.key, s.key, s.s_date FROM fullouter_long_big_1a_nonull b FULL OUTER JOIN fullouter_long_small_1a s ON b.key = s.key
order by b.key;

-- Small table without NULL key(s).
SELECT b.key, s.key, s.s_date FROM fullouter_long_big_1a b FULL OUTER JOIN fullouter_long_small_1a_nonull s ON b.key = s.key
order by b.key;

-- Both Big and Small tables without NULL key(s).
SELECT b.key, s.key, s.s_date FROM fullouter_long_big_1a_nonull b FULL OUTER JOIN fullouter_long_small_1a_nonull s ON b.key = s.key
order by b.key;


CREATE TABLE fullouter_long_big_1b(key smallint)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_long_big_1b.txt' OVERWRITE INTO TABLE fullouter_long_big_1b;

CREATE TABLE fullouter_long_small_1b(key smallint, s_timestamp timestamp)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_long_small_1b.txt' OVERWRITE INTO TABLE fullouter_long_small_1b;

analyze table fullouter_long_big_1b compute statistics;
analyze table fullouter_long_big_1b compute statistics for columns;
analyze table fullouter_long_small_1b compute statistics;
analyze table fullouter_long_small_1b compute statistics for columns;

EXPLAIN VECTORIZATION DETAIL
SELECT b.key, s.key, s.s_timestamp FROM fullouter_long_big_1b b FULL OUTER JOIN fullouter_long_small_1b s ON b.key = s.key
order by b.key;

SELECT b.key, s.key, s.s_timestamp FROM fullouter_long_big_1b b FULL OUTER JOIN fullouter_long_small_1b s ON b.key = s.key
order by b.key;


CREATE TABLE fullouter_long_big_1c(key int, b_string string)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_long_big_1c.txt' OVERWRITE INTO TABLE fullouter_long_big_1c;

CREATE TABLE fullouter_long_small_1c(key int, s_decimal decimal(38, 18))
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_long_small_1c.txt' OVERWRITE INTO TABLE fullouter_long_small_1c;

analyze table fullouter_long_big_1c compute statistics;
analyze table fullouter_long_big_1c compute statistics for columns;
analyze table fullouter_long_small_1c compute statistics;
analyze table fullouter_long_small_1c compute statistics for columns;

EXPLAIN VECTORIZATION DETAIL
SELECT b.key, b.b_string, s.key, s.s_decimal FROM fullouter_long_big_1c b FULL OUTER JOIN fullouter_long_small_1c s ON b.key = s.key
order by b.key;

SELECT b.key, b.b_string, s.key, s.s_decimal FROM fullouter_long_big_1c b FULL OUTER JOIN fullouter_long_small_1c s ON b.key = s.key
order by b.key;


CREATE TABLE fullouter_long_big_1d(key int)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_long_big_1d.txt' OVERWRITE INTO TABLE fullouter_long_big_1d;

CREATE TABLE fullouter_long_small_1d(key int)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_long_small_1d.txt' OVERWRITE INTO TABLE fullouter_long_small_1d;

analyze table fullouter_long_big_1d compute statistics;
analyze table fullouter_long_big_1d compute statistics for columns;
analyze table fullouter_long_small_1d compute statistics;
analyze table fullouter_long_small_1d compute statistics for columns;

EXPLAIN VECTORIZATION DETAIL
SELECT b.key, s.key FROM fullouter_long_big_1d b FULL OUTER JOIN fullouter_long_small_1d s ON b.key = s.key
order by b.key;

SELECT b.key, s.key FROM fullouter_long_big_1d b FULL OUTER JOIN fullouter_long_small_1d s ON b.key = s.key
order by b.key;


------------------------------------------------------------------------------------------
-- MULTI-KEY key
------------------------------------------------------------------------------------------

CREATE TABLE fullouter_multikey_big_1a_txt(key0 smallint, key1 int)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_multikey_big_1a.txt' OVERWRITE INTO TABLE fullouter_multikey_big_1a_txt;
CREATE TABLE fullouter_multikey_big_1a STORED AS ORC AS SELECT * FROM fullouter_multikey_big_1a_txt;

CREATE TABLE fullouter_multikey_big_1a_nonull_txt(key0 smallint, key1 int)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_multikey_big_1a_nonull.txt' OVERWRITE INTO TABLE fullouter_multikey_big_1a_nonull_txt;
CREATE TABLE fullouter_multikey_big_1a_nonull STORED AS ORC AS SELECT * FROM fullouter_multikey_big_1a_nonull_txt;

CREATE TABLE fullouter_multikey_small_1a_txt(key0 smallint, key1 int)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_multikey_small_1a.txt' OVERWRITE INTO TABLE fullouter_multikey_small_1a_txt;
CREATE TABLE fullouter_multikey_small_1a STORED AS ORC AS SELECT * FROM fullouter_multikey_small_1a_txt;

CREATE TABLE fullouter_multikey_small_1a_nonull_txt(key0 smallint, key1 int)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_multikey_small_1a_nonull.txt' OVERWRITE INTO TABLE fullouter_multikey_small_1a_nonull_txt;
CREATE TABLE fullouter_multikey_small_1a_nonull STORED AS ORC AS SELECT * FROM fullouter_multikey_small_1a_nonull_txt;

analyze table fullouter_multikey_big_1a compute statistics;
analyze table fullouter_multikey_big_1a compute statistics for columns;
analyze table fullouter_multikey_big_1a_nonull compute statistics;
analyze table fullouter_multikey_big_1a_nonull compute statistics for columns;
analyze table fullouter_multikey_small_1a compute statistics;
analyze table fullouter_multikey_small_1a compute statistics for columns;
analyze table fullouter_multikey_small_1a_nonull compute statistics;
analyze table fullouter_multikey_small_1a_nonull compute statistics for columns;


EXPLAIN VECTORIZATION DETAIL
SELECT b.key0, b.key1, s.key0, s.key1 FROM fullouter_multikey_big_1a b FULL OUTER JOIN fullouter_multikey_small_1a s ON b.key0 = s.key0 AND b.key1 = s.key1
order by b.key0, b.key1;

SELECT b.key0, b.key1, s.key0, s.key1 FROM fullouter_multikey_big_1a b FULL OUTER JOIN fullouter_multikey_small_1a s ON b.key0 = s.key0 AND b.key1 = s.key1
order by b.key0, b.key1;

-- Big table without NULL key(s).
SELECT b.key0, b.key1, s.key0, s.key1 FROM fullouter_multikey_big_1a_nonull b FULL OUTER JOIN fullouter_multikey_small_1a s ON b.key0 = s.key0 AND b.key1 = s.key1
order by b.key0, b.key1;

-- Small table without NULL key(s).
SELECT b.key0, b.key1, s.key0, s.key1 FROM fullouter_multikey_big_1a b FULL OUTER JOIN fullouter_multikey_small_1a_nonull s ON b.key0 = s.key0 AND b.key1 = s.key1
order by b.key0, b.key1;

-- Both Big and Small tables without NULL key(s).
SELECT b.key0, b.key1, s.key0, s.key1 FROM fullouter_multikey_big_1a_nonull b FULL OUTER JOIN fullouter_multikey_small_1a_nonull s ON b.key0 = s.key0 AND b.key1 = s.key1
order by b.key0, b.key1;




CREATE TABLE fullouter_multikey_big_1b_txt(key0 timestamp, key1 smallint, key2 string)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_multikey_big_1b.txt' OVERWRITE INTO TABLE fullouter_multikey_big_1b_txt;
CREATE TABLE fullouter_multikey_big_1b STORED AS ORC AS SELECT * FROM fullouter_multikey_big_1b_txt;

CREATE TABLE fullouter_multikey_small_1b_txt(key0 timestamp, key1 smallint, key2 string, s_decimal decimal(38, 18))
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_multikey_small_1b.txt' OVERWRITE INTO TABLE fullouter_multikey_small_1b_txt;
CREATE TABLE fullouter_multikey_small_1b STORED AS ORC AS SELECT * FROM fullouter_multikey_small_1b_txt;

analyze table fullouter_multikey_big_1b_txt compute statistics;
analyze table fullouter_multikey_big_1b_txt compute statistics for columns;
analyze table fullouter_multikey_small_1b_txt compute statistics;
analyze table fullouter_multikey_small_1b_txt compute statistics for columns;

EXPLAIN VECTORIZATION DETAIL
SELECT b.key0, b.key1, b.key2, s.key0, s.key1, s.key2, s.s_decimal FROM fullouter_multikey_big_1b b FULL OUTER JOIN fullouter_multikey_small_1b s ON b.key0 = s.key0 AND b.key1 = s.key1 AND b.key2 = s.key2
order by b.key0, b.key1;

SELECT b.key0, b.key1, b.key2, s.key0, s.key1, s.key2, s.s_decimal FROM fullouter_multikey_big_1b b FULL OUTER JOIN fullouter_multikey_small_1b s ON b.key0 = s.key0 AND b.key1 = s.key1 AND b.key2 = s.key2
order by b.key0, b.key1;


------------------------------------------------------------------------------------------
-- Single STRING key
------------------------------------------------------------------------------------------

CREATE TABLE fullouter_string_big_1a_txt(key string)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_string_big_1a.txt' OVERWRITE INTO TABLE fullouter_string_big_1a_txt;
CREATE TABLE fullouter_string_big_1a STORED AS ORC AS SELECT * FROM fullouter_string_big_1a_txt;

CREATE TABLE fullouter_string_big_1a_nonull_txt(key string)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_string_big_1a_nonull.txt' OVERWRITE INTO TABLE fullouter_string_big_1a_nonull_txt;
CREATE TABLE fullouter_string_big_1a_nonull STORED AS ORC AS SELECT * FROM fullouter_string_big_1a_nonull_txt;

CREATE TABLE fullouter_string_small_1a_txt(key string, s_date date, s_timestamp timestamp)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_string_small_1a.txt' OVERWRITE INTO TABLE fullouter_string_small_1a_txt;
CREATE TABLE fullouter_string_small_1a STORED AS ORC AS SELECT * FROM fullouter_string_small_1a_txt;

CREATE TABLE fullouter_string_small_1a_nonull_txt(key string, s_date date, s_timestamp timestamp)
row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../../data/files/fullouter_string_small_1a_nonull.txt' OVERWRITE INTO TABLE fullouter_string_small_1a_nonull_txt;
CREATE TABLE fullouter_string_small_1a_nonull STORED AS ORC AS SELECT * FROM fullouter_string_small_1a_nonull_txt;

analyze table fullouter_string_big_1a compute statistics;
analyze table fullouter_string_big_1a compute statistics for columns;
analyze table fullouter_string_big_1a_nonull compute statistics;
analyze table fullouter_string_big_1a_nonull compute statistics for columns;
analyze table fullouter_string_small_1a compute statistics;
analyze table fullouter_string_small_1a compute statistics for columns;
analyze table fullouter_string_small_1a_nonull compute statistics;
analyze table fullouter_string_small_1a_nonull compute statistics for columns;


EXPLAIN VECTORIZATION DETAIL
SELECT b.key, s.key, s.s_date, s.s_timestamp FROM fullouter_string_big_1a b FULL OUTER JOIN fullouter_string_small_1a s ON b.key = s.key
order by b.key;

SELECT b.key, s.key, s.s_date, s.s_timestamp FROM fullouter_string_big_1a b FULL OUTER JOIN fullouter_string_small_1a s ON b.key = s.key
order by b.key;

-- Big table without NULL key(s).
SELECT b.key, s.key, s.s_date, s.s_timestamp FROM fullouter_string_big_1a_nonull b FULL OUTER JOIN fullouter_string_small_1a s ON b.key = s.key
order by b.key;

-- Small table without NULL key(s).
SELECT b.key, s.key, s.s_date, s.s_timestamp FROM fullouter_string_big_1a b FULL OUTER JOIN fullouter_string_small_1a_nonull s ON b.key = s.key
order by b.key;

-- Both Big and Small tables without NULL key(s).
SELECT b.key, s.key, s.s_date, s.s_timestamp FROM fullouter_string_big_1a_nonull b FULL OUTER JOIN fullouter_string_small_1a_nonull s ON b.key = s.key
order by b.key;



