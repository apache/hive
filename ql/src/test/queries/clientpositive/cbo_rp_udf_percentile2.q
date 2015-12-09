set hive.cbo.returnpath.hiveop=true;

DESCRIBE FUNCTION percentile;
DESCRIBE FUNCTION EXTENDED percentile;


set hive.map.aggr = false;
set hive.groupby.skewindata = false;

-- SORT_QUERY_RESULTS

SELECT CAST(key AS INT) DIV 10,
       count(distinct(value)),
       percentile(CAST(substr(value, 5) AS INT), 0.0),
       percentile(CAST(substr(value, 5) AS INT), 0.5),
       percentile(CAST(substr(value, 5) AS INT), 1.0),
       percentile(CAST(substr(value, 5) AS INT), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;

SELECT CAST(key AS INT) DIV 10,
       count(distinct(value)),
       percentile(CAST(substr(value, 5) AS INT), 0.0),
       count(distinct(substr(value, 5))),
       percentile(CAST(substr(value, 5) AS INT), 0.5),
       percentile(CAST(substr(value, 5) AS INT), 1.0),
       percentile(CAST(substr(value, 5) AS INT), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;


SELECT CAST(key AS INT) DIV 10,
       count(distinct(value)),
       percentile(CAST(substr(value, 5) AS INT), 0.0),
       count(distinct(substr(value, 5))),
       percentile(CAST(substr(value, 5) AS INT), 0.5),
       count(distinct(substr(value, 2))),
       percentile(CAST(substr(value, 5) AS INT), 1.0),
       count(distinct(CAST(key AS INT) DIV 10)),
       percentile(CAST(substr(value, 5) AS INT), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;
