set hive.cli.print.header=true;
set hive.explain.user=false;
SET hive.auto.convert.join=true;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;

-- SORT_QUERY_RESULTS

create table vectortab2k(
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

LOAD DATA LOCAL INPATH '../../data/files/vectortab2k' OVERWRITE INTO TABLE vectortab2k;

CREATE TABLE scratch AS SELECT t, si, i, b, bo FROM vectortab2k;
INSERT INTO TABLE scratch VALUES (NULL, NULL, NULL, NULL, NULL);

CREATE TABLE vectortab2k_orc STORED AS ORC AS SELECT * FROM scratch;

SET hive.vectorized.execution.enabled=true;

EXPLAIN
SELECT sum(hash(*)) FROM
    (SELECT t, si, i, (t < 0) as child1, (si > 0) as child2, (i < 0) as child3, (t < 0 AND si > 0 AND i < 0) as multi_and_col from vectortab2k_orc
        order by t, si, i) as q;

SELECT sum(hash(*)) FROM
    (SELECT t, si, i, (t < 0) as child1, (si > 0) as child2, (i < 0) as child3, (t < 0 AND si > 0 AND i < 0) as multi_and_col from vectortab2k_orc
        order by t, si, i) as q;

EXPLAIN
SELECT sum(hash(*)) FROM
    (SELECT t, si, i, b, (t < 0) as child1, (si > 0) as child2, (i < 0) as child3, (b > 0) as child4, (t < 0 AND si > 0 AND i < 0 AND b > 0) as multi_and_col from vectortab2k_orc
        order by t, si, i, b) as q;

SELECT sum(hash(*)) FROM
    (SELECT t, si, i, b, (t < 0) as child1, (si > 0) as child2, (i < 0) as child3, (b > 0) as child4, (t < 0 AND si > 0 AND i < 0 AND b > 0) as multi_and_col from vectortab2k_orc
        order by t, si, i, b) as q;

-- Use a boolean column rather than a column comparison expression.
EXPLAIN
SELECT sum(hash(*)) FROM
    (SELECT t, si, bo, b, (t < 0) as child1, (si > 0) as child2, (bo) as child3, (b > 0) as child4, (t < 0 AND si > 0 AND bo AND b > 0) as multi_and_col from vectortab2k_orc
        order by t, si, bo, b) as q;

SELECT sum(hash(*)) FROM
    (SELECT t, si, bo, b, (t < 0) as child1, (si > 0) as child2, (bo) as child3, (b > 0) as child4, (t < 0 AND si > 0 AND bo AND b > 0) as multi_and_col from vectortab2k_orc
        order by t, si, bo, b) as q;

-- With some filtering
SELECT sum(hash(*)) FROM
    (SELECT t, si, i, (t < 0) as child1, (si > 0) as child2, (i < 0) as child3, (t < 0 AND si > 0 AND i < 0) as multi_and_col from vectortab2k_orc
        where pmod(t, 4) > 1
        order by t, si, i) as q;
SELECT sum(hash(*)) FROM
    (SELECT t, si, i, b, (t < 0) as child1, (si > 0) as child2, (i < 0) as child3, (b > 0) as child4, (t < 0 AND si > 0 AND i < 0 AND b > 0) as multi_and_col from vectortab2k_orc
        where pmod(t, 4) < 2
        order by t, si, i, b) as q;
SELECT sum(hash(*)) FROM
    (SELECT t, si, bo, b, (t < 0) as child1, (si > 0) as child2, (bo) as child3, (b > 0) as child4, (t < 0 AND si > 0 AND bo AND b > 0) as multi_and_col from vectortab2k_orc
        where pmod(b, 8) == 7
        order by t, si, bo, b) as q;

SET hive.vectorized.execution.enabled=false;

CREATE TABLE scratch_repeat AS SELECT t, si, i, b, bo, 20 as t_repeat,
     9000 as si_repeat, 9233320 as i_repeat, -823823999339992 as b_repeat, false as bo_repeat_false, true as bo_repeat_true FROM vectortab2k;

-- The repeated columns ought to create repeated VectorizedRowBatch for those columns.
-- And then when we do a comparison, we should generate a repeated boolean result.
CREATE TABLE vectortab2k_orc_repeat STORED AS ORC AS SELECT * FROM scratch_repeat;

SET hive.vectorized.execution.enabled=true;

-- t_repeat > 0 should generate all true.
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si, i, (t_repeat > 0) as child1, (si > 0) as child2, (i < 0) as child3, (t_repeat > 0 AND si > 0 AND i < 0) as multi_and_col from vectortab2k_orc_repeat
        order by t_repeat, si, i) as q;

-- t_repeat < 0 should generate all false.
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si, i, (t_repeat < 0) as child1, (si > 0) as child2, (i < 0) as child3, (t_repeat < 0 AND si > 0 AND i < 0) as multi_and_col from vectortab2k_orc_repeat
        order by t_repeat, si, i) as q;

-- Two repeated false columns at beginning...
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si_repeat, i, (t_repeat > 0) as child1, (si_repeat > 0) as child2, (i < 0) as child3, (t_repeat > 0 AND si_repeat > 0 AND i < 0) as multi_and_col from vectortab2k_orc_repeat
        order by t_repeat, si_repeat, i) as q;

-- si_repeat > 0 should generate all true.
SELECT sum(hash(*)) FROM
    (SELECT t, si_repeat, i, b_repeat, (t < 0 ) as child1, (si_repeat > 0) as child2, (i < 0) as child3, (b_repeat > 0) as child4, (t < 0 AND si_repeat > 0 AND i < 0 AND b_repeat > 0) as multi_and_col from vectortab2k_orc_repeat
        order by t, si_repeat, i, b_repeat) as q;

-- si_repeat < 0 should generate all false.
SELECT sum(hash(*)) FROM
    (SELECT t, si_repeat, i, b_repeat, (t < 0) as child1, (si_repeat < 0) as child2, (i < 0) as child3, (b_repeat > 0) as child4, (t < 0 AND si_repeat < 0 AND i < 0 AND b_repeat > 0) as multi_and_col from vectortab2k_orc_repeat
        order by t, si_repeat, i, b_repeat) as q;

-- Use a boolean column rather than a column comparison expression.
SELECT sum(hash(*)) FROM
    (SELECT t, si, bo_repeat_false, b, (t < 0) as child1, (si > 0) as child2, (bo_repeat_false) as child3, (b > 0) as child4, (t < 0 AND si > 0 AND bo_repeat_false AND b > 0) as multi_and_col from vectortab2k_orc_repeat
        order by t, si, bo_repeat_false, b) as q;

SELECT sum(hash(*)) FROM
    (SELECT t, si, bo_repeat_true, b, (t < 0) as child1, (si > 0) as child2, (bo_repeat_true) as child3, (b > 0) as child4, (t < 0 AND si > 0 AND bo_repeat_true AND b > 0) as multi_and_col from vectortab2k_orc_repeat
        order by t, si, bo_repeat_true, b) as q;

-- With some filtering
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si, i, (t_repeat > 0) as child1, (si > 0) as child2, (i < 0) as child3, (t_repeat > 0 AND si > 0 AND i < 0) as multi_and_col from vectortab2k_orc_repeat
        where pmod(si, 4) = 0
        order by t_repeat, si, i) as q;
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si, i, (t_repeat < 0) as child1, (si > 0) as child2, (i < 0) as child3, (t_repeat < 0 AND si > 0 AND i < 0) as multi_and_col from vectortab2k_orc_repeat
        where pmod(si, 4) = 3
        order by t_repeat, si, i) as q;
SELECT sum(hash(*)) FROM
    (SELECT t_repeat, si_repeat, i, (t_repeat > 0) as child1, (si_repeat > 0) as child2, (i < 0) as child3, (t_repeat > 0 AND si_repeat > 0 AND i < 0) as multi_and_col from vectortab2k_orc_repeat
        where pmod(si, 4) != 3
        order by t_repeat, si_repeat, i) as q;
SELECT sum(hash(*)) FROM
    (SELECT t, si_repeat, i, b_repeat, (t < 0 ) as child1, (si_repeat > 0) as child2, (i < 0) as child3, (b_repeat > 0) as child4, (t < 0 AND si_repeat > 0 AND i < 0 AND b_repeat > 0) as multi_and_col from vectortab2k_orc_repeat
        where pmod(si, 4) < 2
        order by t, si_repeat, i, b_repeat) as q;
SELECT sum(hash(*)) FROM
    (SELECT t, si_repeat, i, b_repeat, (t < 0) as child1, (si_repeat < 0) as child2, (i < 0) as child3, (b_repeat > 0) as child4, (t < 0 AND si_repeat < 0 AND i < 0 AND b_repeat > 0) as multi_and_col from vectortab2k_orc_repeat
        where pmod(t, 4) = 0
        order by t, si_repeat, i, b_repeat) as q;
SELECT sum(hash(*)) FROM
    (SELECT t, si, bo_repeat_false, b, (t < 0) as child1, (si > 0) as child2, (bo_repeat_false) as child3, (b > 0) as child4, (t < 0 AND si > 0 AND bo_repeat_false AND b > 0) as multi_and_col from vectortab2k_orc_repeat
        where pmod(b, 4) > 1
        order by t, si, bo_repeat_false, b) as q;
SELECT sum(hash(*)) FROM
    (SELECT t, si, bo_repeat_true, b, (t < 0) as child1, (si > 0) as child2, (bo_repeat_true) as child3, (b > 0) as child4, (t < 0 AND si > 0 AND bo_repeat_true AND b > 0) as multi_and_col from vectortab2k_orc_repeat
        where pmod(si, 4) < 3
        order by t, si, bo_repeat_true, b) as q;


SET hive.vectorized.execution.enabled=false;

CREATE TABLE scratch_null AS SELECT t, si, i, b, bo,
     cast(null as tinyint) as t_null, cast(null as smallint) as si_null, cast(null as int) as i_null, cast(null as bigint) as b_null, cast(null as boolean) as bo_null FROM vectortab2k;

-- The nulled columns ought to create repeated null VectorizedRowBatch for those columns.
CREATE TABLE vectortab2k_orc_null STORED AS ORC AS SELECT * FROM scratch_null;

SET hive.vectorized.execution.enabled=true;

SELECT sum(hash(*)) FROM
    (SELECT t_null, si, i, (t_null is null) as child1, (si > 0) as child2, (i < 0) as child3, (t_null is null AND si > 0 AND i < 0) as multi_and_col from vectortab2k_orc_null
        order by t_null, si, i) as q;

SELECT sum(hash(*)) FROM
    (SELECT t_null, si, i, (t_null < 0) as child1, (si > 0) as child2, (i < 0) as child3, (t_null < 0 AND si > 0 AND i < 0) as multi_and_col from vectortab2k_orc_null
        order by t_null, si, i) as q;

SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i, (t_null is null) as child1, (si_null > 0) as child2, (i < 0) as child3, (t_null is null AND si_null > 0 AND i < 0) as multi_and_col from vectortab2k_orc_null
        order by t_null, si_null, i) as q;
    
SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i_null, (t_null is null) as child1, (si_null is null) as child2, (i_null is null) as child3, (t_null is null AND si_null is null AND i_null is null) as multi_and_col from vectortab2k_orc_null
        order by t_null, si_null, i_null) as q;

-- With some filtering
SELECT sum(hash(*)) FROM
    (SELECT t_null, si, i, (t_null is null) as child1, (si > 0) as child2, (i < 0) as child3, (t_null is null AND si > 0 AND i < 0) as multi_and_col from vectortab2k_orc_null
        where pmod(i,4) = 3
        order by t_null, si, i) as q;
SELECT sum(hash(*)) FROM
    (SELECT t_null, si, i, (t_null < 0) as child1, (si > 0) as child2, (i < 0) as child3, (t_null < 0 AND si > 0 AND i < 0) as multi_and_col from vectortab2k_orc_null
        where pmod(i,4) = 2
        order by t_null, si, i) as q;
SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i, (t_null is null) as child1, (si_null > 0) as child2, (i < 0) as child3, (t_null is null AND si_null > 0 AND i < 0) as multi_and_col from vectortab2k_orc_null
        where pmod(i,4) != 3
        order by t_null, si_null, i) as q;
SELECT sum(hash(*)) FROM
    (SELECT t_null, si_null, i_null, (t_null is null) as child1, (si_null is null) as child2, (i_null is null) as child3, (t_null is null AND si_null is null AND i_null is null) as multi_and_col from vectortab2k_orc_null
        where pmod(i,4) < 3
        order by t_null, si_null, i_null) as q;

