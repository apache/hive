set hive.cli.print.header=true;
set hive.explain.user=false;
SET hive.auto.convert.join=true;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;

-- SORT_QUERY_RESULTS

create table vectortab2k_n6(
            t tinyint,
            si smallint,
            i int,
            b bigint,
            f float,
            d double,
            dc decimal(38,18),
            bo boolean,
            s string,
            s2 string,
            ts timestamp,
            ts2 timestamp,
            dt date)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/vectortab2k' OVERWRITE INTO TABLE vectortab2k_n6;

CREATE TABLE scratch AS SELECT t, si, i, b, f, d, dc FROM vectortab2k_n6;
INSERT INTO TABLE scratch VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE vectortab2k_orc STORED AS ORC AS SELECT * FROM scratch;

SET hive.vectorized.execution.enabled=true;

--
-- Projection LongCol<Compare>LongScalar
--
EXPLAIN VECTORIZATION EXPRESSION
SELECT sum(hash(*)) FROM
    (SELECT t, si, i, (t < 0) as compare1, (si <= 0) as compare2, (i = 0) as compare3 from vectortab2k_orc
        order by t, si, i) as q;

SELECT sum(hash(*)) FROM
    (SELECT t, si, i, (t < 0) as compare1, (si <= 0) as compare2, (i = 0) as compare3 from vectortab2k_orc
        order by t, si, i) as q;

EXPLAIN VECTORIZATION EXPRESSION
SELECT sum(hash(*)) FROM
    (SELECT t, si, i, b, (t > 0) as compare1, (si >= 0) as compare2, (i != 0) as compare3, (b > 0) as compare4 from vectortab2k_orc
        order by t, si, i, b) as q;

SELECT sum(hash(*)) FROM
    (SELECT t, si, i, b, (t > 0) as compare1, (si >= 0) as compare2, (i != 0) as compare3, (b > 0) as compare4 from vectortab2k_orc
        order by t, si, i, b) as q;

-- With some filtering
SELECT sum(hash(*)) FROM
    (SELECT t, si, i, (t < 0) as compare1, (si <= 0) as compare2, (i = 0) as compare3 from vectortab2k_orc
        where pmod(t, 4) > 1
        order by t, si, i) as q;
SELECT sum(hash(*)) FROM
    (SELECT t, si, i, b, (t > 0) as compare1, (si >= 0) as compare2, (i != 0) as compare3, (b > 0) as compare4 from vectortab2k_orc
        where pmod(t, 4) < 2
        order by t, si, i, b) as q;


--
-- Projection LongScalar<Compare>LongColumn
--
SELECT sum(hash(*)) FROM
    (SELECT t, si, i, (0 < t) as compare1, (0 <= si) as compare2, (0 = i) as compare3 from vectortab2k_orc
        order by t, si, i) as q;

SELECT sum(hash(*)) FROM
    (SELECT t, si, i, b, (0 > t) as compare1, (0 >= si) as compare2, (0 != i) as compare3, (0 > b) as compare4 from vectortab2k_orc
        order by t, si, i, b) as q;

-- With some filtering
SELECT sum(hash(*)) FROM
    (SELECT t, si, i, (0 < t) as compare1, (0 <= si) as compare2, (0 = i) as compare3 from vectortab2k_orc
        where pmod(t, 4) > 1
        order by t, si, i) as q;
SELECT sum(hash(*)) FROM
    (SELECT t, si, i, b, (0 > t) as compare1, (0 >= si) as compare2, (0 != i) as compare3, (0 > b) as compare4 from vectortab2k_orc
        where pmod(t, 4) < 2
        order by t, si, i, b) as q;

SET hive.vectorized.execution.enabled=false;

CREATE TABLE scratch_repeat AS SELECT t, si, i, b, bo, 20 as t_repeat,
     9000 as si_repeat, 9233320 as i_repeat, -823823999339992 as b_repeat, false as bo_repeat_false, true as bo_repeat_true FROM vectortab2k_n6;

-- The repeated columns ought to create repeated VectorizedRowBatch for those columns.
-- And then when we do a comparison, we should generate a repeated boolean result.
CREATE TABLE vectortab2k_orc_repeat STORED AS ORC AS SELECT * FROM scratch_repeat;

SET hive.vectorized.execution.enabled=true;

--
-- Projection LongCol<Compare>LongScalar
--
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si_repeat, i_repeat, (t_repeat > 0) as compare1, (si_repeat >= 0) as compare2, (i_repeat = 0) as compare3 from vectortab2k_orc_repeat
        order by t_repeat, si_repeat, i_repeat) as q;

SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si_repeat, i_repeat, (t_repeat < 0) as compare1, (si_repeat <=0) as compare2, (i_repeat != 0) as compare3 from vectortab2k_orc_repeat
        order by t_repeat, si_repeat, i_repeat) as q;

-- With some filtering
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si_repeat, i_repeat, (t_repeat > 0) as compare1, (si_repeat >= 0) as compare2, (i_repeat = 0) as compare3 from vectortab2k_orc_repeat
        where pmod(si, 4) = 0
        order by t_repeat, si_repeat, i_repeat) as q;
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si_repeat, i_repeat, (t_repeat < 0) as compare1, (si_repeat <=0) as compare2, (i_repeat != 0) as compare3 from vectortab2k_orc_repeat
        where pmod(si, 4) = 3
        order by t_repeat, si_repeat, i_repeat) as q;

--
-- Projection LongScalar<Compare>LongColumn
--
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si_repeat, i_repeat, (0 > t_repeat) as compare1, (0 >= si_repeat) as compare2, (0 = i_repeat) as compare3 from vectortab2k_orc_repeat
        order by t_repeat, si_repeat, i_repeat) as q;

SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si_repeat, i_repeat, (0 < t_repeat) as compare1, (0 <= si_repeat) as compare2, (0 != i_repeat) as compare3 from vectortab2k_orc_repeat
        order by t_repeat, si_repeat, i_repeat) as q;

-- With some filtering
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si_repeat, i_repeat, (0 > t_repeat) as compare1, (0 >= si_repeat) as compare2, (0 = i_repeat) as compare3 from vectortab2k_orc_repeat
        where pmod(si, 4) = 0
        order by t_repeat, si_repeat, i_repeat) as q;
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si_repeat, i_repeat, (0 < t_repeat) as compare1, (0 <= si_repeat) as compare2, (0 != i_repeat) as compare3 from vectortab2k_orc_repeat
        where pmod(si, 4) = 3
        order by t_repeat, si_repeat, i_repeat) as q;

SET hive.vectorized.execution.enabled=false;

CREATE TABLE scratch_null AS SELECT t, si, i, b, bo,
     cast(null as tinyint) as t_null, cast(null as smallint) as si_null, cast(null as int) as i_null, cast(null as bigint) as b_null, cast(null as boolean) as bo_null FROM vectortab2k_n6;

-- The nulled columns ought to create repeated null VectorizedRowBatch for those columns.
CREATE TABLE vectortab2k_orc_null STORED AS ORC AS SELECT * FROM scratch_null;

SET hive.vectorized.execution.enabled=true;

--
-- Projection LongCol<Compare>LongScalar
--
SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i_null, (t_null > 0) as compare1, (si_null >= 0) as compare2, (i_null = 0) as compare3 from vectortab2k_orc_null
        order by t_null, si_null, i_null) as q;

SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i_null, (t_null < 0) as compare1, (si_null <=0) as compare2, (i_null != 0) as compare3 from vectortab2k_orc_null
        order by t_null, si_null, i_null) as q;

-- With some filtering
SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i_null, (t_null > 0) as compare1, (si_null >= 0) as compare2, (i_null = 0) as compare3 from vectortab2k_orc_null
        where pmod(si, 4) = 0
        order by t_null, si_null, i_null) as q;
SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i_null, (t_null < 0) as compare1, (si_null <=0) as compare2, (i_null != 0) as compare3 from vectortab2k_orc_null
        where pmod(si, 4) = 3
        order by t_null, si_null, i_null) as q;

--
-- Projection LongScalar<Compare>LongColumn
--
SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i_null, (0 > t_null) as compare1, (0 >= si_null) as compare2, (0 = i_null) as compare3 from vectortab2k_orc_null
        order by t_null, si_null, i_null) as q;

SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i_null, (0 < t_null) as compare1, (0 <= si_null) as compare2, (0 != i_null) as compare3 from vectortab2k_orc_null
        order by t_null, si_null, i_null) as q;

-- With some filtering
SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i_null, (0 > t_null) as compare1, (0 >= si_null) as compare2, (0 = i_null) as compare3 from vectortab2k_orc_null
        where pmod(si, 4) = 0
        order by t_null, si_null, i_null) as q;
SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i_null, (0 < t_null) as compare1, (0 <= si_null) as compare2, (0 != i_null) as compare3 from vectortab2k_orc_null
        where pmod(si, 4) = 3
        order by t_null, si_null, i_null) as q;

