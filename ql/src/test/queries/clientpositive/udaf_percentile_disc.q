--! qt:dataset:src
DESCRIBE FUNCTION percentile_disc;
DESCRIBE FUNCTION EXTENDED percentile_disc;


set hive.map.aggr = false;
set hive.groupby.skewindata = false;

-- SORT_QUERY_RESULTS

SELECT CAST(key AS INT) DIV 10,
       percentile_disc(CAST(substr(value, 5) AS INT), 0.0),
       percentile_disc(CAST(substr(value, 5) AS DOUBLE), 0.5),
       percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(substr(value, 5) AS INT)),
       percentile_disc(CAST(substr(value, 5) AS DECIMAL), 1.0),
       percentile_disc(array(0.0, 0.5, 1.0)) WITHIN GROUP (ORDER BY CAST(substr(value, 5) AS DOUBLE))
FROM src
GROUP BY CAST(key AS INT) DIV 10;


set hive.map.aggr = true;
set hive.groupby.skewindata = false;

SELECT CAST(key AS INT) DIV 10,
       percentile_disc(CAST(substr(value, 5) AS INT), 0.0),
       percentile_disc(CAST(substr(value, 5) AS DOUBLE), 0.5),
       percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(substr(value, 5) AS INT)),
       percentile_disc(CAST(substr(value, 5) AS DECIMAL), 1.0),
       percentile_disc(array(0.0, 0.5, 1.0)) WITHIN GROUP (ORDER BY CAST(substr(value, 5) AS DOUBLE))
FROM src
GROUP BY CAST(key AS INT) DIV 10;



set hive.map.aggr = false;
set hive.groupby.skewindata = true;

SELECT CAST(key AS INT) DIV 10,
       percentile_disc(CAST(substr(value, 5) AS INT), 0.0),
       percentile_disc(CAST(substr(value, 5) AS DOUBLE), 0.5),
       percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(substr(value, 5) AS INT)),
       percentile_disc(CAST(substr(value, 5) AS DECIMAL), 1.0),
       percentile_disc(array(0.0, 0.5, 1.0)) WITHIN GROUP (ORDER BY CAST(substr(value, 5) AS DOUBLE))
FROM src
GROUP BY CAST(key AS INT) DIV 10;


set hive.map.aggr = true;
set hive.groupby.skewindata = true;

SELECT CAST(key AS INT) DIV 10,
       percentile_disc(CAST(substr(value, 5) AS INT), 0.0),
       percentile_disc(CAST(substr(value, 5) AS DOUBLE), 0.5),
       percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(substr(value, 5) AS INT)),
       percentile_disc(CAST(substr(value, 5) AS DECIMAL), 1.0),
       percentile_disc(array(0.0, 0.5, 1.0)) WITHIN GROUP (ORDER BY CAST(substr(value, 5) AS DOUBLE))
FROM src
GROUP BY CAST(key AS INT) DIV 10;


set hive.map.aggr = true;
set hive.groupby.skewindata = false;

-- test null handling
SELECT CAST(key AS INT) DIV 10,
       percentile_disc(NULL, 0.0),
       percentile_disc(0.0) WITHIN GROUP (ORDER BY NULL)
FROM src
GROUP BY CAST(key AS INT) DIV 10;


-- test empty array handling
SELECT CAST(key AS INT) DIV 10,
       percentile_disc(IF(CAST(key AS INT) DIV 10 < 5, 1, NULL), 0.5),
       percentile_disc(0.5) WITHIN GROUP (ORDER BY IF(CAST(key AS INT) DIV 10 < 5, 1, NULL))
FROM src
GROUP BY CAST(key AS INT) DIV 10;

explain cbo
select percentile_disc(cast(key as bigint), 0.5),
       percentile_disc(0.5) within group (order by cast(key as bigint))
from src where false;

select percentile_disc(cast(key as bigint), 0.5),
       percentile_disc(0.5) within group (order by cast(key as bigint))
from src where false;


CREATE TABLE t_test (value int);
INSERT INTO t_test VALUES (NULL), (3), (8), (13), (7), (6), (20), (NULL), (NULL), (10), (7), (15), (16), (8), (7), (8), (NULL);

EXPLAIN SELECT
percentile_disc(value, 0.0),
percentile_disc(value, 0.2),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value NULLS FIRST),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value NULLS LAST),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value) = percentile_disc(value, 0.2),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value ASC),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value ASC) = percentile_disc(value, 0.2),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value DESC),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value DESC NULLS FIRST),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value DESC NULLS LAST)
FROM t_test;

SELECT
percentile_disc(value, 0.0),
percentile_disc(value, 0.2),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value NULLS FIRST),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value NULLS LAST),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value) = percentile_disc(value, 0.2),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value ASC),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value ASC) = percentile_disc(value, 0.2),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value DESC),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value DESC NULLS FIRST),
percentile_disc(0.2) WITHIN GROUP (ORDER BY value DESC NULLS LAST)
FROM t_test;

DROP TABLE t_test;
