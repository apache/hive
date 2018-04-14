--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.map.aggr=false;
set hive.groupby.skewindata=true;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n137(key STRING, val1 INT, val2 INT) partitioned by (ds string);
CREATE TABLE dest2_n36(key STRING, val1 INT, val2 INT) partitioned by (ds string);

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n137 partition(ds='111')
  SELECT src.value, count(src.key), count(distinct src.key) GROUP BY src.value
INSERT OVERWRITE TABLE dest2_n36  partition(ds='111')
  SELECT substr(src.value, 5), count(src.key), count(distinct src.key) GROUP BY substr(src.value, 5);

FROM src
INSERT OVERWRITE TABLE dest1_n137 partition(ds='111')
  SELECT src.value, count(src.key), count(distinct src.key) GROUP BY src.value
INSERT OVERWRITE TABLE dest2_n36  partition(ds='111')
  SELECT substr(src.value, 5), count(src.key), count(distinct src.key) GROUP BY substr(src.value, 5);

SELECT * from dest1_n137;
SELECT * from dest2_n36;



