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
    (SELECT IF(((t < 0) OR (si > 0)),t,IF(((b > 0) OR (bo)),t*2,t*3)) AS `if_expr` FROM vectortab2k_orc
        order by if_expr) as q;

SELECT sum(hash(*)) FROM
    (SELECT IF(((t < 0) OR (si > 0)),t,IF(((b > 0) OR (bo)),t*2,t*3)) AS `if_expr` FROM vectortab2k_orc
        order by if_expr) as q;


SET hive.vectorized.execution.enabled=false;

CREATE TABLE scratch_bool AS SELECT t % 4 = 0 as bool0, si % 2 = 1 as bool1, i % 5 = 3 as bool2, b % 3 = 2 as bool3, bo as bool4, true as bool5 FROM vectortab2k;
INSERT INTO TABLE scratch_bool VALUES (NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE bool_orc STORED AS ORC AS SELECT * FROM scratch_bool;

SET hive.vectorized.execution.enabled=true;

EXPLAIN
SELECT sum(hash(*)) FROM
    (SELECT bool0, not bool0 as not_bool0, bool1, not bool1 as not_bool1, ((bool0 and (not bool1)) or (bool1 and (not bool0))) as multi_and_or_col from bool_orc
        order by bool0, bool1) as q;

SELECT sum(hash(*)) FROM
    (SELECT bool0, not bool0 as not_bool0, bool1, not bool1 as not_bool1, ((bool0 and (not bool1)) or (bool1 and (not bool0))) as multi_and_or_col from bool_orc
        order by bool0, bool1) as q;


EXPLAIN
SELECT sum(hash(*)) FROM
    (SELECT bool0, not bool1 as not_bool1, bool2, not bool3 as not_bool3, ((bool0 and (not bool1)) or (bool2 and (not bool3))) as multi_and_or_col from bool_orc
        order by bool0, not_bool1, bool2, not_bool3) as q;

SELECT sum(hash(*)) FROM
    (SELECT bool0, not bool1 as not_bool1, bool2, not bool3 as not_bool3, ((bool0 and (not bool1)) or (bool2 and (not bool3))) as multi_and_or_col from bool_orc
        order by bool0, not_bool1, bool2, not_bool3) as q;


EXPLAIN
SELECT sum(hash(*)) FROM
    (SELECT bool0, not bool1 as not_bool1, bool2, not bool3 as not_bool3, ((bool0 and (not bool1)) or (bool2 and (not bool3)) or (bool2 and (not bool5))) as multi_and_or_col from bool_orc
        order by bool0, not_bool1, bool2, not_bool3) as q;

SELECT sum(hash(*)) FROM
    (SELECT bool0, not bool1 as not_bool1, bool2, not bool3 as not_bool3, ((bool0 and (not bool1)) or (bool2 and (not bool3)) or (bool2 and (not bool5))) as multi_and_or_col from bool_orc
        order by bool0, not_bool1, bool2, not_bool3) as q;
