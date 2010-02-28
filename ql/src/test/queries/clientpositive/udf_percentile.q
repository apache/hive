DESCRIBE FUNCTION percentile;
DESCRIBE FUNCTION EXTENDED percentile;


set hive.map.aggr = false;
set hive.groupby.skewindata = false;

SELECT CAST(key AS INT) DIV 10,
       percentile(CAST(substr(value, 5) AS INT), 0.0),
       percentile(CAST(substr(value, 5) AS INT), 0.5),
       percentile(CAST(substr(value, 5) AS INT), 1.0),
       percentile(CAST(substr(value, 5) AS INT), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;


set hive.map.aggr = true;
set hive.groupby.skewindata = false;

SELECT CAST(key AS INT) DIV 10,
       percentile(CAST(substr(value, 5) AS INT), 0.0),
       percentile(CAST(substr(value, 5) AS INT), 0.5),
       percentile(CAST(substr(value, 5) AS INT), 1.0),
       percentile(CAST(substr(value, 5) AS INT), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;



set hive.map.aggr = false;
set hive.groupby.skewindata = true;

SELECT CAST(key AS INT) DIV 10,
       percentile(CAST(substr(value, 5) AS INT), 0.0),
       percentile(CAST(substr(value, 5) AS INT), 0.5),
       percentile(CAST(substr(value, 5) AS INT), 1.0),
       percentile(CAST(substr(value, 5) AS INT), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;


set hive.map.aggr = true;
set hive.groupby.skewindata = true;

SELECT CAST(key AS INT) DIV 10,
       percentile(CAST(substr(value, 5) AS INT), 0.0),
       percentile(CAST(substr(value, 5) AS INT), 0.5),
       percentile(CAST(substr(value, 5) AS INT), 1.0),
       percentile(CAST(substr(value, 5) AS INT), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;
