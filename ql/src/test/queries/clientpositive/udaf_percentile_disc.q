--! qt:dataset:src
DESCRIBE FUNCTION percentile_disc;
DESCRIBE FUNCTION EXTENDED percentile_disc;


set hive.map.aggr = false;
set hive.groupby.skewindata = false;

-- SORT_QUERY_RESULTS

SELECT CAST(key AS INT) DIV 10,
       percentile_disc(CAST(substr(value, 5) AS INT), 0.0),
       percentile_disc(CAST(substr(value, 5) AS DOUBLE), 0.5),
       percentile_disc(CAST(substr(value, 5) AS DECIMAL), 1.0)
FROM src
GROUP BY CAST(key AS INT) DIV 10;


set hive.map.aggr = true;
set hive.groupby.skewindata = false;

SELECT CAST(key AS INT) DIV 10,
       percentile_disc(CAST(substr(value, 5) AS INT), 0.0),
       percentile_disc(CAST(substr(value, 5) AS DOUBLE), 0.5),
       percentile_disc(CAST(substr(value, 5) AS DECIMAL), 1.0)
FROM src
GROUP BY CAST(key AS INT) DIV 10;



set hive.map.aggr = false;
set hive.groupby.skewindata = true;

SELECT CAST(key AS INT) DIV 10,
       percentile_disc(CAST(substr(value, 5) AS INT), 0.0),
       percentile_disc(CAST(substr(value, 5) AS DOUBLE), 0.5),
       percentile_disc(CAST(substr(value, 5) AS DECIMAL), 1.0)
FROM src
GROUP BY CAST(key AS INT) DIV 10;


set hive.map.aggr = true;
set hive.groupby.skewindata = true;

SELECT CAST(key AS INT) DIV 10,
       percentile_disc(CAST(substr(value, 5) AS INT), 0.0),
       percentile_disc(CAST(substr(value, 5) AS DOUBLE), 0.5),
       percentile_disc(CAST(substr(value, 5) AS DECIMAL), 1.0)
FROM src
GROUP BY CAST(key AS INT) DIV 10;


set hive.map.aggr = true;
set hive.groupby.skewindata = false;

-- test null handling
SELECT CAST(key AS INT) DIV 10,
       percentile_disc(NULL, 0.0)
FROM src
GROUP BY CAST(key AS INT) DIV 10;


-- test empty array handling
SELECT CAST(key AS INT) DIV 10,
       percentile_disc(IF(CAST(key AS INT) DIV 10 < 5, 1, NULL), 0.5)
FROM src
GROUP BY CAST(key AS INT) DIV 10;

select percentile_disc(cast(key as bigint), 0.5) from src where false;
